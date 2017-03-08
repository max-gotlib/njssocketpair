//
//  NJSSocketPair.js
//  njsocketpair
//
//  Created by Maxim Gotlib on 2/28/17.
//  Copyright © 2017 Maxim Gotlib. All rights reserved.
//

const assert = require('assert');
const net = require('net');
const fs = require('fs');
const path = require('path');
const util = require('util');
const debuglog = util.debuglog('SP');

/**
 * Pure JS implementation of UNIX socketpair() function for Node.js.
 *
 * Connected socket pair is represented by SocketPair class instances. Each instance has "local" and "remote" socket properties, though there is no actual difference between those sockets.
 * Socket couple is obtained by an attempt to connect "local" socket to a Net.Server, accepting connection on UNIX-domain or named-pipes (under Win32). Successful connection attempt results in a SocketPair instantiated. All the procedures are synchronous.
 *
 * SocketPair class exposes the following API.
 *
 * SocketPair.socketpair(callback)
 *      Start asynchronous connected socket pair acquisition. Upon procedure completion, provided callback function is called.
 *      Sockets are allocated from UNIX domain or (under Window) represent named-pipe handles.
 *      Callback function arguments:
 *          - sp Allocated socket pair (SocketPair class) instance or undefined on error;
 *          - error Error descriptor or undefined on success.
 *
 * SocketPair.pendingPairingRequets
 *      Number of socket pairing requests, being currently processed.
 *
 * SocketPair.stopListener([force,] [callback])
 *      Stop local socket listener, if one is currently running.
 *      Parameters:
 *          - force (optional) If passed as true, then all local sockets, waiting for pairing completed, will be forcibly closed;
 *          - callback (optional) Callback function to call on listener stopped. Callback has no arguments.
 *
 * SocketPair.listenerPathname
 *      Pathname for binding local socket listener, used to acquire connected socket pairs.
 *
 * SocketPair.pairingTimeout
 *      Timeout (milliseconds) for socket pair acquisition.
 *
 * Usage example:
 *
 *      const njsp = require('./njssocketpair/lib/NJSSocketPair.js');
 *
 *      console.log(`Listener sock path: ${njsp.listenerPathname}`);
 *
 *       njsp.socketpair((sp, err) => {
 *          console.log('In socketpair() callback.');
 *          if(!sp) {
 *              console.error(err);
 *              return;
 *          }
 *
 *          console.log('Istablishing ping-pong.');
 *
 *          sp.remoteSock
 *              .on('data', (data) => {
 *                  console.log('Received from "local": ' + data.toString());
 *                  sp.remoteSock.end('Good morning to you too!');
 *              })
 *              .resume();
 *
 *          sp.localSock
 *              .on('data', (data) => {
 *                  console.log('Received from "remote": ' + data.toString());
 *                  sp.localSock.end();
 *              })
 *              .resume()
 *              .write('Hello world!');
 *      });
 *
 *      njsp.stopListener(false, () => { console.log("Stopped."); });
 */
class SocketPair {
    
    /**
     * Designated constructor for socket pair instances.
     *
     * @param localSock Local socket object;
     * @param remoteSock Remote socket object.
     */
    constructor(localSock, remoteSock) {
        this._localSock = localSock;
        this._remoteSock = remoteSock;
    }
    
    // Public API.
    
    /**
     * Getter for local socket property.
     *
     * @return Local socket object, associated with this socket pair.
     */
    get localSock() {
        return this._localSock;
    }

    /**
     * Getter for remote socket property.
     *
     * @return Remote socket object, associated with this socket pair.
     */
    get remoteSock() {
        return this._remoteSock;
    }

    /**
     * Obtain a pair of connected socket objects.
     * Sockets are allocated from UNIX domain or (under Window) represent named-pipe handles.
     * One of couple members is notated as 'local' and another - as a "remote" socket, though there is no actual difference between those sockets.
     *
     * @param callback (required) Callback method, called on socket pair allocation completed. Callback has two parameters:
     *  - sp Allocated socket pair instance or undefined on error;
     *  - error Error descriptor or undefined on success.
     */
    static socketpair(callback) {
        assert(callback, "Asynchronous SocketPair.socketpair() method called with no callback.");
        if(!SocketPair._requestCount) {
            SocketPair._requestCount = 1;
        } else {
            SocketPair._requestCount += 1;
        }
        
        // Start socket acceptor server if not yet started.
        SocketPair._startListener((pipeSrv, error) => {
            if(!pipeSrv) {
                callback(undefined, error);
                return;
            }

            let localSock = new net.Socket()
            .on('error', (error) => {
                debuglog('Local socket connect: %s', error);
                callback(undefined, error);
                SocketPair._stopIfPossibleAndNeededOnRequestCompleted();
            })
            .once('close', () => {
                debuglog('Local socket closed.');
                SocketPair._dequeueLocalSocket(localSock);
                callback(undefined, new Error('Premature local socket closure.'));
                SocketPair._stopIfPossibleAndNeededOnRequestCompleted();
            })
            .once('paired', (sp) => {
                assert(sp != undefined);
                assert(sp.localSock === localSock);
                localSock.removeAllListeners('error');
                localSock.removeAllListeners('close');
                debuglog('Completed socket pair allocation: %d/%d', localSock._handle.fd, sp._remoteSock._handle.fd);
                callback(sp, undefined);
                SocketPair._stopIfPossibleAndNeededOnRequestCompleted();
            })
            .connect(SocketPair.listenerPathname, () => {
                let fd = SocketPair._enqueueLocalSocket(localSock);
                debuglog('Connected local sock with fd: %d.', fd);
                let b = Buffer.alloc(8);
                b.writeInt32LE(fd);
                b.writeInt32LE(pipeSrv._pipeCookie, 4);
                localSock.write(b);
            });
        });
    }

    /**
     * Getter for class property - the number of socket pairing requests, being currently processed.
     */
    static get pendingPairingRequets() {
        return SocketPair._requestCount === undefined ? 0 : SocketPair._requestCount;
    }
    
    /**
     * Stop local socket listener, if one is currently running.
     *
     * @param force (optional) If passed as true, then all local sockets, waiting for pairing completed, will be forcibly closed.
     * @param callback (optional) Callback function to call on listener stopped. Callback has no arguments.
     */
    static stopListener(force, callback) {
        if(callback === undefined) {
            if(typeof force == 'function') {
                callback = force;
                force = true;
            }
        } else if(force === undefined) {
            force = false;
        }
        
        if(SocketPair._listener == undefined) {
            if(callback) {
                callback();
            }
            return;
        }
        
        if(!force && SocketPair._requestCount)
        {
            debuglog('Queued local socket listener termination.');
            SocketPair._stopRequested = true;
            SocketPair._listener.once('close', callback);
            return;
        }
        
        debuglog('Stopping local socket listener.');
        SocketPair._listener.close((err) => {
            debuglog('Local socket listener is stopped.');
            if(callback) {
                callback();
            }
        });
        
        if(force) {
            let q = SocketPair._socketQueue;
            if(q && q.size) {
                debuglog('Closing local sockets, being paired: %s', q);
                q.forEach((sock, fd) => {
                    sock.destroy('SocketPair: Listener stopped');
                });
                delete SocketPair._socketQueue;
            }
        }
        delete SocketPair._listener;
    }

    // Protected API.

    /**
     * Getter for listening socket pathname - static (class) property.
     *
     * @return Pathname for binding local socket listener, used to acquire connected socket pairs.
     */
    static get listenerPathname() {
        if(SocketPair._listenerPathname === undefined) {
            if( process.platform == 'win32' ) {
                SocketPair._listenerPathname = path.join('\\\\?\\pipe', process.cwd(), 'SocketPair-' + process.pid);
                debuglog('Will use Win32 named pipe family with pathname: %s', SocketPair._listenerPathname);
            } else {
                SocketPair._listenerPathname = path.join('/tmp', path.basename(process.execPath) + '-SocketPair-' + process.pid + '.sock');
                debuglog('Will use UNIX socket family with pathname: %s', SocketPair._listenerPathname);
            }
        }
        return SocketPair._listenerPathname;
    }

    /**
     * Getter for local socket listener instance - static (class) property.
     * Local socket listener is an instance of Net.Server class, used to acquire connected socket pairs.
     *
     * @return Local socket listener instance, used to acquire connected socket pairs.
     */
    static get listener() {
        return SocketPair._listener;
    }

    // Private API.
    
    /**
     * Start, if needed, local socket listener.
     *
     * @param callback (optional) Callback function to call on listener start attempt completed. Callback has two parameters:
     *  - listener: started listener (Net.Server) instance or undefined on error;
     *  - err: error object or undefined, if listener started successfully.
     */
    static _startListener(callback) {
        if(SocketPair._listener === undefined) {
            SocketPair._unlinkListenerSocket();
            
            debuglog('Starting local socket listener.');
            const pipeSrv = net.createServer()
            .on('listening', () => {
                debuglog('Local socket listener is ready.');
            })
            .on('error', (err) => {
                debuglog('Local socket listener: %s', err);
                let p = SocketPair._listener;
                delete SocketPair._listener;
                p.close();
            })
            .on('close', () => {
                debuglog('Local socket listener closed.');
                SocketPair._unlinkListenerSocket();
            })
            .on('connection', (pipeSock) => {
                let sp = new SocketPair();
                sp._pipeCookie = pipeSrv._pipeCookie;
                
                pipeSock.pause()
                .once('error', (err) => {
                    debuglog('Remote socket error: %s', err);
                    pipeSock.destroy();
                    pipeSock = undefined;
                })
                .once('close', () => {
                    debuglog('Remote socket closed.');
                    pipeSock.destroy();
                    pipeSock = undefined;
                })
                .on('readable', () => {
                    // Read local socket handle (file descriptor) value over the connection.
                    let localFd = sp._consumeLocalSockFdHandleVal(pipeSock);
                    if(localFd === undefined) {
                        return;
                    }
                    pipeSock.setTimeout(0);
                    debuglog('Reported local sock fd is: %d.', localFd);
                    pipeSock.removeAllListeners('readable');
                    pipeSock.removeAllListeners('error');
                    pipeSock.removeAllListeners('close');
                    // Use peer socket handle to couple a socket pair.
                    SocketPair._dequeueLocalSocket(localFd, (localSock) => {
                        if(localSock === undefined) {
                            debuglog('No local sock fd in the pending coupling queue: ' + localFd);
                            pipeSock.destroy();
                            return;
                        }
                        localSock.pause();
                        debuglog('Paired local/remote: %d/%d', localFd, pipeSock._handle.fd);
                        sp._localSock = localSock;
                        sp._remoteSock = pipeSock;
                        localSock.emit('paired', sp);
                    });
                })
                .setTimeout(exports.pairingTimeout, () => {
                    debuglog('Timed out, waiting for local socket introduce itself.');
                    pipeSock.destroy();
                    pipeSock = undefined;
                });
            });
            pipeSrv._pipeCookie = parseInt(Math.random().toString(10).slice(2, 10));
            SocketPair._listener = pipeSrv;
            pipeSrv.listen(SocketPair.listenerPathname, 256);
        }
        
        if(!callback) {
            return;
        }
        
        if(SocketPair._listener.listening) {
            callback(SocketPair._listener, undefined);
        } else {
            SocketPair._listener
            .once('listening', () => { callback(SocketPair._listener, undefined); })
            .once('error', (e) => { callback(undefined, e); });
        }
        return SocketPair._listener;
    }

    /**
     * Check the number of socket pairing requests, being processed, and if it is zero and there was a listener termination request queued - stop the local socket listener.
     */
    static _stopIfPossibleAndNeededOnRequestCompleted() {
        if(SocketPair._requestCount) {
            SocketPair._requestCount -= 1;
        }
        if(!SocketPair._stopRequested || SocketPair._requestCount)
        {
            return;
        }
        SocketPair.stopListener(false);
    }

    /**
     * Unkink local socket, used by listener to couple socket pair. This method is relevant only, if UNIX domain sockets used.
     */
    static _unlinkListenerSocket() {
        if(process.platform != 'win32')
        {
            try {
                let pathname = SocketPair.listenerPathname;
                fs.accessSync(pathname, fs.constants.F_OK);
                fs.unlinkSync(pathname);
                debuglog('Unlinked listener socket at: %s', pathname);
            } catch(e) {
            }
        }
    }
    
    /**
     * Add local socket to the queue of sockets, waiting for getting paired.
     *
     * @param sock Socket to add to the queue. Socket should be already connected (should hace handle assigned).
     * @return Socket handle value (file descriptor) or undefined on error.
     */
    static _enqueueLocalSocket(sock) {
        assert(sock != undefined && sock._handle);
        let fd = sock._handle.fd;
        if(fd != undefined) {
            if(SocketPair._socketQueue === undefined) {
                SocketPair._socketQueue = new Map();
            }
            assert(SocketPair._socketQueue.get(fd) === undefined, 'Already enqueued handle: ' + fd);
            SocketPair._socketQueue.set(fd, sock);
        }
        return fd;
    }

    /**
     * Lookup a local socket in the queue of sockets, waiting for pairing. On success, remove the socket from the queue.
     *
     * @param fd Socket handle, used as the key for local socket lookup.
     * @param callback (optional) Callback function to call on lookup completed. Callback argument - local socket, corresponding to given handle, or undefined on unsuccessful lookup.
     */
    static _dequeueLocalSocket(fd, callback) {
        assert(fd != undefined);
        if(SocketPair._socketQueue === undefined) {
            if(callback) {
                callback(undefined);
            }
            return;
        }
        if(typeof fd == 'object') {
            let realFd = fd._handle ? fd._handle.fd : undefined;
            if(!realFd) {
                // Wortht case - there is no socket handle and a sequential lookup is necessary.
                for( var [key, value] of SocketPair._socketQueue) {
                    if(value === fd) {
                        SocketPair._socketQueue.delete(key);
                        if(callback) {
                            callback(sock);
                        }
                        return;
                    }
                }
            }
            if(callback) {
                callback(undefined);
            }
            return;
        }
        let sock = SocketPair._socketQueue.get(fd);
        SocketPair._socketQueue.delete(fd);
        if(callback) {
            callback(sock);
        }
    }
    
    /**
     * Process incoming data, received by pipe socket and collect 8-byte buffer. This buffer carries local socket handle (32-bit unsigned integer - file descriptor, and 32-bit random cookie, used to filter out aliens) value and will be used for local-remote sockets coupling.
     *
     * @param sock "Remote" socket to read data from.
     * @return Local socket handle (file descriptor) value, received by "remote" socket. Undefined is returned on input data deficiency.
     */
    _consumeLocalSockFdHandleVal(sock) {
        let data = sock.read(8);
        if(data === null) {
            return undefined;
        }
        if(data.length < 8) {
            return undefined;
        }
        let cookie = data.readInt32LE(4);
        assert(this._pipeCookie);
        if(cookie != this._pipeCookie) {
            debuglog('Incoming connection havn\'t provided valid cookie: %d, expected: %d.', cookie, this._pipeCookie);
            sock.end();
            return undefined;
        }
        let fd = data.readInt32LE(0);
        return fd;
    }
}

var exports = module.exports = {};
exports.SocketPair = SocketPair;
exports.socketpair = SocketPair.socketpair;
exports.stopListener = SocketPair.stopListener;
exports.listenerPathname = SocketPair.listenerPathname;
exports.pairingTimeout = 500;
