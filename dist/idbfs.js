"use strict";

/**
 * IDBFS Wrapper for synchronization and persistence.
 * This module provides a way to synchronize an IndexedDB-based file system (IDBFS)
 * with remote storage or other listeners.
 */

const textEncoder = new TextEncoder();

/**
 * Writes a 32-bit unsigned integer to a Uint8Array in little-endian format.
 * @param {Uint8Array} buffer 
 * @param {number} value 
 * @param {number} offset 
 * @returns {number} New offset
 */
function writeUint32(buffer, value, offset) {
    buffer[offset] = value & 0xFF;
    buffer[offset + 1] = (value & 0xFF00) >> 8;
    buffer[offset + 2] = (value & 0xFF0000) >> 16;
    buffer[offset + 3] = (value & 0xFF000000) >> 24;
    return offset + 4;
}

/**
 * Reads a 32-bit unsigned integer from a Uint8Array in little-endian format.
 * @param {Uint8Array} buffer 
 * @param {number} offset 
 * @returns {number}
 */
function readUint32(buffer, offset) {
    return (buffer[offset] & 0xFF) |
           ((buffer[offset + 1] << 8) & 0xFF00) |
           ((buffer[offset + 2] << 16) & 0xFF0000) |
           ((buffer[offset + 3] << 24) & 0xFF000000);
}

/**
 * Writes a 64-bit timestamp (milliseconds) to a Uint8Array as two 32-bit integers.
 * @param {Uint8Array} buffer 
 * @param {number} value 
 * @param {number} offset 
 * @returns {number} New offset
 */
function writeUint64(buffer, value, offset) {
    const high = (value / 0x100000000) >>> 0;
    const low = value >>> 0;
    writeUint32(buffer, low, offset);
    writeUint32(buffer, high, offset + 4);
    return offset + 8;
}

/**
 * Reads a 64-bit timestamp from a Uint8Array.
 * @param {Uint8Array} buffer 
 * @param {number} offset 
 * @returns {number}
 */
function readUint64(buffer, offset) {
    const low = readUint32(buffer, offset);
    const high = readUint32(buffer, offset + 4);
    return (high * 0x100000000) + low;
}

/**
 * Deserializes a binary buffer into an array of file entries.
 * Format: [pathLen(4)][path(pathLen)][timestamp(8)][mode(4)][hasContents(1)][contentsLen(4)?][contents?]
 * @param {Uint8Array} buffer 
 * @returns {Array}
 */
function deserializeEntries(buffer) {
    const entries = [];
    let offset = 0;
    while (offset < buffer.length) {
        const pathLength = readUint32(buffer, offset);
        offset += 4;
        
        const path = textDecoder.decode(buffer.subarray(offset, offset + pathLength));
        offset += pathLength;
        
        const timestamp = readUint64(buffer, offset);
        offset += 8;
        
        const mode = readUint32(buffer, offset);
        offset += 4;
        
        const hasContents = buffer[offset] === 1;
        offset += 1;
        
        let contents;
        if (hasContents) {
            const contentsLength = readUint32(buffer, offset);
            offset += 4;
            contents = buffer.subarray(offset, offset + contentsLength);
            offset += contentsLength;
        }
        
        entries.push({
            path: path,
            timestamp: new Date(timestamp),
            mode: mode,
            contents: contents
        });
    }
    return entries;
}

/**
 * Serializes an array of file entries into a binary buffer.
 * @param {Array} entries 
 * @returns {Uint8Array}
 */
function serializeEntries(entries) {
    let totalSize = 0;
    const preparedEntries = entries.map(entry => {
        const encodedPath = textEncoder.encode(entry.path);
        let entrySize = 4 + encodedPath.length + 8 + 4 + 1;
        if (entry.contents) {
            entrySize += 4 + entry.contents.length;
        }
        totalSize += entrySize;
        return {
            encodedPath: encodedPath,
            timestamp: entry.timestamp.getTime(),
            mode: entry.mode,
            contents: entry.contents
        };
    });

    const buffer = new Uint8Array(totalSize);
    let offset = 0;
    for (const entry of preparedEntries) {
        offset = writeUint32(buffer, entry.encodedPath.length, offset);
        buffer.set(entry.encodedPath, offset);
        offset += entry.encodedPath.length;
        
        offset = writeUint64(buffer, entry.timestamp, offset);
        offset = writeUint32(buffer, entry.mode, offset);
        
        buffer[offset] = entry.contents ? 1 : 0;
        offset += 1;
        
        if (entry.contents) {
            offset = writeUint32(buffer, entry.contents.length, offset);
            buffer.set(entry.contents, offset);
            offset += entry.contents.length;
        }
    }
    return buffer;
}

/**
 * Wraps an IDBFS instance to provide synchronization capabilities.
 * @param {function} logger (logger func)
 */
function wrapIDBFS(logger) {
    const onLoadListeners = [];
    const onSaveListeners = [];

    /**
     * Helper to get the IndexedDB database instance for a mountpoint.
     */
    function getDatabase(idbfs_instance, mountpoint) {
        return new Promise((resolve, reject) => {
            idbfs_instance.getDB(mountpoint, (err, db) => {
                if (err) return reject(err);
                resolve(db);
            });
        });
    }

    /**
     * Saves remote entries into the local IndexedDB.
     */
    async function saveRemoteEntries(idbfs_instance, mountpoint, entries) {
        const db = await getDatabase(idbfs_instance, mountpoint);
        return new Promise((resolve, reject) => {
            (async function() {
                const transaction = db.transaction([idbfs_instance.DB_STORE_NAME], "readwrite");
                const store = transaction.objectStore(idbfs_instance.DB_STORE_NAME);
                
                for (const entry of entries) {
                    await new Promise((res, rej) => {
                        idbfs_instance.storeRemoteEntry(store, entry.path, entry, (err) => {
                            if (err) return rej(err);
                            res();
                        });
                    });
                }
                
                transaction.onerror = (event) => {
                    reject(event);
                    event.preventDefault();
                };
                transaction.oncomplete = () => {
                    resolve();
                };
            })().catch(reject);
        });
    }

    /**
     * Clears all entries in the local IndexedDB for a mountpoint.
     */
    async function clearDatabase(idbfs_instance, mountpoint) {
        const db = await getDatabase(mountpoint);
        const transaction = db.transaction([idbfs_instance.DB_STORE_NAME], "readwrite");
        const store = transaction.objectStore(idbfs_instance.DB_STORE_NAME);
        
        await new Promise((resolve, reject) => {
            const request = store.clear();
            request.onsuccess = () => resolve();
            request.onerror = () => reject(request.error);
        });
    }

    /**
     * Main synchronization function.
     * Replaces the default idbfs_instance.syncfs.
     */
    window.syncfs = (idbfs_instance, mountpoint, isRemoteToLocal, onComplete, onReady) => {
        (async function() {
            // Phase 1: Load from remote to local
            if (isRemoteToLocal) {
                for (const listener of onLoadListeners) {
                    const data = await listener(idbfs_instance, mountpoint);
                    if (data) {
                        await clearDatabase(idbfs_instance, mountpoint);
                        if (data.length > 0) {
                            const entries = deserializeEntries(data);
                            await saveRemoteEntries(idbfs_instance, mountpoint, entries);
                        }
                        // Stop after the first listener that provides data
                        break;
                    }
                }
            }

            // Phase 2: Signal readiness and handle local-to-remote sync
            onReady((callback) => {
                // Call the original completion callback
                onComplete(callback);
                
                (async function() {
                    // If we are doing local-to-remote sync
                    if (!isRemoteToLocal) {
                        let cachedSerializedData = null;
                        
                        /**
                         * Lazy-serializes local entries for listeners.
                         */
                        const getSerializedData = async () => {
                            if (cachedSerializedData === null) {
                                const localEntries = await (async function() {
                                    const localSet = await new Promise((resolve, reject) => {
                                        idbfs_instance.getLocalSet(mountpoint, (err, set) => {
                                            if (err) return reject(err);
                                            resolve(set);
                                        });
                                    });
                                    
                                    const entries = [];
                                    for (const path of Object.keys(localSet.entries)) {
                                        const entry = await new Promise((resolve, reject) => {
                                            idbfs_instance.loadLocalEntry(path, (err, e) => {
                                                if (err) return reject(err);
                                                resolve(e);
                                            });
                                        });
                                        entry.path = path;
                                        entries.push(entry);
                                    }
                                    return entries;
                                })();
                                
                                cachedSerializedData = serializeEntries(localEntries);
                            }
                            return cachedSerializedData;
                        };

                        // Notify all save listeners
                        for (const listener of onSaveListeners) {
                            listener(getSerializedData, idbfs_instance, mountpoint);
                        }
                    }
                })().catch(err => {
                    logger("ERR!!! syncfs error", err);
                });
            });
        })().catch(err => {
            logger("ERR!!! syncfs error", err);
            onComplete(err);
        });
    };

    return {
        /**
         * Adds a listener for load and save events.
         * @param {Object} listener { onLoad: function, onSave: function }
         */
        addListener: (listener) => {
            if (listener.onLoad) onLoadListeners.push(listener.onLoad);
            if (listener.onSave) onSaveListeners.push(listener.onSave);
        }
    };
}

// Export to window
window.wrapIDBFS = wrapIDBFS;
