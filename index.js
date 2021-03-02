 /*jshint bitwise: false*/
 'use strict';

 (function (root, factory) {
     if (typeof define === 'function' && define.amd) {
         // AMD. Register as an anonymous module.
         define(factory);
     } else {
         // Browser globals
         root.DBSYNC = factory();
     }
 }(this, function () {

    var DBSYNC = {
        serverUrl: null,
        sizeMax: 1048576,
        segmento: 1000,
        db: null,
        tablesToSync: [],//eg.  [{tableName : 'myDbTable', idName : 'myTable_id'},{tableName : 'stat'}]
        idNameFromTableName : {}, //map to get the idName with the tableName (key)
        syncInfo: {//this object can have other useful info for the server ex. {deviceId : "XXXX", email : "fake@g.com"}
            lastSyncDate : null// attribute managed by webSqlSync
        },
        syncResult: null,
        SqlTranError: null,
        firstSync: {},
        firstSyncDate : 0,
        keyStr: 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=',
        cbEndSync: null,
        clientData: null,
        serverData: null,
        syncDate:null,

        username: null, // basic authentication support
        password: null, // basic authentication support

        /*************** PUBLIC FUNCTIONS ********************/
        /**
         * Initialize the synchronization (should be called before any call to syncNow)
         * (it will create automatically the necessary tables and triggers if needed)
         * @param {Object} theTablesToSync : ex : [{ tableName: 'card_stat', idName: 'card_id'}, {tableName: 'stat'}] //no need to precise id if the idName is "id".
         * @param {Object} dbObject : the WebSQL database object.
         * @param {Object} theSyncInfo : will be sent to the server (useful to store any ID or device info).
         * @param {Object} theServerUrl
         * @param {Object} callBack(firstInit) : called when init finished.
         * @param {Object} username : username for basci authentication support
         * @param {Object} password : password for basci authentication support
         */
        initSync: function(theTablesToSync, dbObject, theSyncInfo, theServerUrl, callBack, username, password) {
            this.db = dbObject;
            this.serverUrl = theServerUrl;
            this.tablesToSync = theTablesToSync;
            this.idNameFromTableName = {};
            this.syncResult = null;
            this.syncInfo = theSyncInfo;
            this.username = username;
            this.password = password;

            // username y password into syncInfo
            if (this.username === null || this.password === null) {
                if (this.syncInfo.username && this.syncInfo.password) {
                    this.username = this.syncInfo.username;
                    this.password = this.syncInfo.password;
                    delete this.syncInfo.username;
                    delete this.syncInfo.password;
                }
            }
            if (this.sizeMax) {
                this.syncInfo.sizeMax = this.sizeMax;
            }
            this.syncInfo.lastSyncDate = {};
            this.init(callBack);
        },

        isRunning: function() {
            if (this.syncResult !== null) {
                return true;
            } else {
                return false;
            }
        },
        isPending: function (callResult) {
            var self = this, sql = '';
            if (self.isRunning()) return 1;
            else {
                sql = 'select count(*) from delete_elem';
                self._selectSql(sql, [], null, function (data) {
                    if (data[0] > 0) callResult(1);
                    else {
                    sql = 'select count(*) from new_elem';
                    self._selectSql(sql, [], null, function (data) {
                            if (data[0] > 0) callResult(1);
                            else callResult(0);
                        });
                    }
                });
            }

        },

        getLastSyncDate: function (tableName) {
            if (tableName) {
                if (this.syncInfo.lastSyncDate.hasOwnProperty(tableName)) {
                return this.syncInfo.lastSyncDate[tableName];
                } else {
                throw new Error(tableName + ' not found.');
                }
            } else {
                let arrLastSyncDate = this._getObjectValues(this.syncInfo.lastSyncDate);
                arrLastSyncDate = arrLastSyncDate.filter(function (elem) { if (elem !== 0) { return elem; } });
                if (arrLastSyncDate.length === 0) {
                return 0;
                } else {
                return arrLastSyncDate.reduce(function (minLast, lastSync) {
                    if (minLast > lastSync) { return lastSync; } else { return minLast; }
                }, Math.round(+new Date(2050, 11, 31) / 1000));
                }
            }
        },

        setSyncDate: function(tableName, val) {
            if (this.syncInfo.lastSyncDate.hasOwnProperty(tableName)) {
                this.syncInfo.lastSyncDate[tableName] = val;
                this._executeSql('UPDATE sync_info SET last_sync = ? where table_name = ?', [val, tableName]);
            } else {
                throw new Error(tableName + ' not found.');
            }
        },

        authBasic: function (username, password) {
          this.username = username;
          this.password = password;
        },

        log: function(message) {
            console.log(message);
        },

        logSql: function(message) {
            console.log(message);
        },

        error: function(message) {
            console.error(message);
        },

        /**
         *
         * @param {function} callBackProgress
         * @param {function} callBackEnd (result.syncOK, result.message).
         * @param {string | string[]} modelsToSync: models to synchronize.
         * @param {boolean} saveBandwidth (default false): if true, the client will not send a request to the server if there is no local changes
        */
        syncNow: function(callBackProgress, callBackEndSync, modelsToSync, saveBandwidth) {

            var self = this, modelsToBackup = [];
            try {            
                if (this.db === null) {
                    self.log('You should call the initSync before (db is null)');
                    throw 'You should call the initSync before (db is null)';
                }
                if (modelsToSync) {
                modelsToBackup = this.checkModelsList(modelsToSync);
                } else {
                for (var cont = 0; cont < self.tablesToSync.length; cont++) {
                    modelsToBackup.push(self.tablesToSync[cont].tableName);
                }
                }
                if (self.syncResult !== null) {
                    callBackEndSync({syncOK: false, codeStr: 'syncInProgress',
                    message: 'synchronization in process', nbSent : 0, nbUpdated:0, nbDeleted:0});
                    return 0;
                } else {
                    self.syncResult = {syncOK: false, codeStr: 'noSync', message: 'No Sync yet',
                        nbSent : 0, nbUpdated:0, nbDeleted:0};
                    //self.syncResult.models = {pendiente = [], completado = []};
                }

                self.cbEndSync = function() {
                    callBackProgress(self.syncResult.message, 100, self.syncResult.codeStr);
                    var resultado = self.syncResult;
                    self.syncResult = null;
                    callBackEndSync(resultado);
                };

                callBackProgress('Getting local data to backup', 0, 'getData');

                self.syncDate = Math.round(new Date().getTime()/1000.0);
                self.firstSyncDate = 0;
                self._syncNowGo(modelsToBackup, callBackProgress, saveBandwidth);
            } catch (error) {
                var resultado = {syncOK: false, codeStr: 'noSync', message: 'No Sync yet',
                nbSent : 0, nbUpdated:0, nbDeleted:0};
                resultado.message = error.message;
                self.syncResult = null;
                callBackEndSync(resultado);
            }

        },


        /******* PRIVATE FUNCTIONS  ******************/

        init: function(callBack) {
            var self = this;
            for (var i = 0; i < self.tablesToSync.length; i++) {
                if (typeof self.tablesToSync[i].idName === 'undefined') {
                    self.tablesToSync[i].idName = 'idsync';//if not specified, the default name is 'id'
                    self.tablesToSync[i].ddl = '';
                }
                self.idNameFromTableName[self.tablesToSync[i].tableName] = self.tablesToSync[i].idName;
            }

            this.db.transaction(function (tx)  {
                self._executeSql('CREATE TABLE IF NOT EXISTS new_elem (table_name TEXT NOT NULL, id TEXT NOT NULL, ' +
                    'change_time TIMESTAMP NOT NULL DEFAULT  (strftime(\'%s\',\'now\')));', [], tx);
                self._executeSql('CREATE INDEX IF NOT EXISTS index_tableName_newElem on new_elem (table_name)');
                self._executeSql('CREATE TABLE IF NOT EXISTS delete_elem (table_name TEXT NOT NULL, id TEXT NOT NULL, ' +
                    'change_time TIMESTAMP NOT NULL DEFAULT  (strftime(\'%s\',\'now\')));', [], tx);
                self._executeSql('CREATE INDEX IF NOT EXISTS index_tableName_deleteElem on delete_elem (table_name)');
                self._executeSql('CREATE TABLE IF NOT EXISTS sync_info (table_name TEXT NOT NULL, last_sync TIMESTAMP);', [], tx);

                // create triggers to automatically fill the new_elem table (this table will contains a pointer to all the modified data)
                self.tablesToSync.forEach(function(curr)  {
                    self._executeSql('CREATE TRIGGER IF NOT EXISTS update_' + curr.tableName + '  AFTER UPDATE ON ' + curr.tableName + ' ' +
                                'WHEN (SELECT last_sync FROM sync_info) > 0 ' +
                                'BEGIN INSERT INTO new_elem (table_name, id) VALUES ' +
                                '("' + curr.tableName + '", new.' + curr.idName + '); END;', [], tx);

                    self._executeSql('CREATE TRIGGER IF NOT EXISTS insert_' + curr.tableName + '  AFTER INSERT ON ' + curr.tableName + ' ' +
                                'WHEN (SELECT last_sync FROM sync_info) > 0 ' +
                                'BEGIN INSERT INTO new_elem (table_name, id) VALUES ' +
                                '("' + curr.tableName + '", new.' + curr.idName + '); END;', [], tx);

                    self._executeSql('CREATE TRIGGER IF NOT EXISTS delete_' + curr.tableName + '  AFTER DELETE ON ' + curr.tableName + ' ' +
                                'BEGIN INSERT INTO delete_elem (table_name, id) VALUES ' +
                                '("' + curr.tableName + '", old.' + curr.idName + '); END;', [], tx);
                    self._getDDLTable(curr.tableName, tx, function (ddl) { curr.ddl = ddl; });
                    self._selectSql('SELECT last_sync FROM sync_info where table_name = ?', [curr.tableName], tx, function (res) {
                        if (res.length === 0 || res[0] === 0) { // First sync (or data lost)
                        if (res.length === 0) {
                            self._executeSql('INSERT OR REPLACE INTO sync_info (table_name, last_sync) VALUES (?,?)', [curr.tableName, 0], tx);
                        }
                        self.firstSync[curr.tableName] = true;
                        self.syncInfo.lastSyncDate[curr.tableName] = 0;
                        } else {
                        self.firstSync[curr.tableName] = false;
                        self.syncInfo.lastSyncDate[curr.tableName] = res[0];
                        if (self.syncInfo.lastSyncDate[curr.tableName] === 0) { self.firstSync[curr.tableName] = true; }
                        }
                    });
                });

            }, function(err) {
                callBack(err);
            }, function () {
                console.log('sync activo.');
                callBack();
            });
        },

        _syncNowGo: function(modelsToBck, callBackProgress, saveBandwidth) {
            var self = this;
            self._getDataToBackup(modelsToBck, function (data) {
                self.clientData = data;
                if (saveBandwidth && self.syncResult.nbSent === 0) {
                    self.syncResult.localDataUpdated = false;
                    self.syncResult.syncOK = true;
                    self.syncResult.codeStr = 'nothingToSend';
                    self.syncResult.message = 'No new data to send to the server';
                    self.cbEndSync();
                    return;
                }

                callBackProgress('Sending ' + self.syncResult.nbSent + ' elements to the server', 20, 'sendData');

                self._sendDataToServer(data, function(serverData) {
                    if (!serverData.data || serverData.result === 'ERROR') {
                        self.syncResult.syncOK = false;
                        if (serverData.status) {
                            self.syncResult.codeStr = serverData.status.toString();
                        } else {
                            self.syncResult.codeStr = 'syncKoServer';
                        }

                        if (serverData.message) {
                            self.syncResult.message = serverData.message;
                        } else {
                            self.syncResult.message = 'Datos obtenidos erroneos.';
                        }
                        self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                        self.error(JSON.stringify(self.syncResult));
                        self.cbEndSync();
                    } else {
                        callBackProgress('Updating local data', 70, 'updateData');
                        if (serverData.data.delete_elem) {
                            self.syncResult.nbDeleted = serverData.data.delete_elem.length;
                        }
                        if (typeof serverData.data === 'undefined' || serverData.data.length === 0) {
                            // nothing to update
                            // We only use the server date to avoid dealing with wrong date from the client
                            self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                            self.syncResult.syncOK = true;
                            self.syncResult.codeStr = 'syncOk';
                            self.syncResult.message = 'First load synchronized successfully. (' + self.syncResult.nbSent +
                                ' new/modified element saved, ' + self.syncResult.nbUpdated + ' updated and ' +
                                self.syncResult.nbDeleted + ' deleted elements.)';
                            self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                            self.cbEndSync();
                            return;
                        }
                        var sqlErrs = [];
                        var counterNbTable = 0;
                        self.serverData = serverData;
                        var nbTables = serverData.models.completado.length;
                        var nbTablesPdte = serverData.models.pendiente.length;
                        var callFinishUpdate =  (table, method, sqlTableErrs) => {
                            if (sqlTableErrs) { sqlErrs = sqlErrs.concat(sqlTableErrs); }
                            counterNbTable++;
                            var perProgress = self._progressRatio(counterNbTable, nbTables, nbTablesPdte);
                            self.log(table.tableName + ' finish,  percent: ' + perProgress.toString());
                            callBackProgress(table.tableName, perProgress, method);
                            if (counterNbTable === nbTables) {
                            if (sqlErrs.length > 0) {
                                self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                                self.syncResult.syncOK = false;
                                self.syncResult.codeStr = 'syncKoData';
                                self.syncResult.message = 'errors found ( ' + sqlErrs.length + ' ) ';
                                sqlErrs.forEach(function (err) { self.syncResult.message += err.message + ' '; });
                                self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                                self.cbEndSync();
                            } else if (serverData.models.pendiente.length === 0)  {
                                self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                                self.syncResult.syncOK = true;
                                self.syncResult.codeStr = 'syncOk';
                                self.syncResult.message = 'First load synchronized successfully. (' + self.syncResult.nbSent +
                                    ' new/modified element saved, ' + self.syncResult.nbUpdated + ' updated and ' +
                                    self.syncResult.nbDeleted + ' deleted elements.)';
                                self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                                self.cbEndSync();
                            } else {
                                self._syncNowGo(serverData.models.pendiente, callBackProgress, saveBandwidth);
                            }
                            }
                        };
                        serverData.models.completado.forEach(function(tableName) {
                            var table = self._getTableToProcess(tableName);
                            var currData = serverData.data[table.tableName] || [];
                            var deleData = serverData.data.delete_elem[table.tableName] || [];
                            if (self.firstSync[table.tableName]) {
                            self._updateFirstLocalDb({ table: table, currData: currData }, callFinishUpdate);
                            } else {
                            self._updateLocalDb({ table: table, currData: currData, deleData: deleData }, callFinishUpdate);
                            }
                        });
                    }
                });
            });
        },
        _getDataToBackup: function(modelsToBck, dataCallBack) {
            var nbData = 0;
            var self = this;
            this.log('_getDataToBackup');
            var dataToSync = {
                info: JSON.parse(JSON.stringify(this.syncInfo)),
                data: {},
                delete_elem: {}
            };
            delete dataToSync.info.lastSyncDate;
            self.db.readTransaction(function (tx) {
            var counter = 0;
            var nbTables = modelsToBck.length;
            dataToSync.info.lastSyncDate = {};
            modelsToBck.forEach(function (tableName) { // a simple for will not work here because we have an asynchronous call inside
                dataToSync.info.lastSyncDate[tableName] = self.syncInfo.lastSyncDate[tableName];
                var currTable = self._getTableToProcess(tableName);
                self._getDataToSavDel(currTable.tableName, currTable.idName, self.firstSync[currTable.tableName], tx, function(data) {
                dataToSync.data[tableName] = data;
                nbData += data.length;
                counter++;
                if (counter === nbTables) {
                    self.log('Data fetched from the local DB');
                    self.syncResult.nbSent += nbData;
                    dataCallBack(dataToSync);
                }
                });
            }); // end for each

            }, function (err) {
            self.log('TransactionError: _getDataToBackup');
            self._errorHandler(undefined , err);
            }, function() {
            self.log('TransactionFinish: _getDataToBackup');
            });
        },
        _finishSync: function (tableName, syncDate,  callBack) {
            var self = this;
            this.firstSync[tableName] = false;
            this.db.transaction(function (tx) {
            self.syncInfo.lastSyncDate[tableName] = syncDate;
            self._executeSql('UPDATE sync_info SET last_sync = ? where  table_name = ?', [syncDate, tableName], tx);
            // Remove only the elem sent to the server (in case new_elem has been added during the sync)
            // We don't do that anymore: this._executeSql('DELETE FROM new_elem', [], tx);

            if (self.clientData.data.hasOwnProperty(tableName)) {
                var idsNewToDelete = [];
                var idsDelToDelete = [];
                var idsString = '';
                var idName =  self.idNameFromTableName[tableName];
                self.clientData.data[tableName].forEach(function (reg) {
                    if (reg.TipoOper === 'U') {
                        idsNewToDelete.push(reg[idName]);
                    } else {
                        idsDelToDelete.push(reg.IdOper);
                    }
                });
                if (idsNewToDelete.length > 0) {
                idsString = idsNewToDelete.map(function(x) { return '?'; }).join(',');
                self._executeSql('DELETE FROM new_elem WHERE table_name =\'' +tableName + '\'' +
                    ' AND id IN (' + idsString + ')' +
                    ' AND change_time <= ' + syncDate, idsNewToDelete, tx);
                }

                if (idsDelToDelete.length > 0) {
                idsString = idsDelToDelete.map(function(x) { return '?'; }).join(',');
                self._executeSql('DELETE FROM delete_elem WHERE table_name =\'' +tableName + '\'' +
                    ' AND id IN (' + idsString + ')' +
                    ' AND change_time <= ' + syncDate, idsDelToDelete, tx);
                }
            }
            },function (err) {
            self._errorHandler(undefined, err);
            delete self.clientData.data[tableName]; // self.clientData = null;
            delete self.serverData.data[tableName];  // self.serverData = null;
            if (callBack) { callBack(); }
            }, function () {
            delete self.clientData.data[tableName]; // self.clientData = null;
            delete self.serverData.data[tableName];  // self.serverData = null;
            if (callBack) { callBack(); }
            });
        },
        _getTableToProcess: function(tableName) {
            var result;
            this.tablesToSync.forEach(function(table) {
                if (table.tableName === tableName) {
                    result = table;
                }
            });
            if (!result) {
            this.error(tableName + ' no se encuentra entre las tablas a sincronizar.');
            }
            return result;
        },
        _getDataToSavDel: function (tableName, idName, needAllData, tx, dataCallBack) {
            var sql = 'select distinct op.TipoOper, op.IdOper , c.* ' +
            'from ( ' +
            'select id IdOper, "U" TipoOper, change_time ' +
            'from new_elem ' +
            'where table_name= ? AND change_time <= ? ' +
            ' union ALL ' +
            'select id IdOper, "D" TipoOper, change_time ' +
            'from delete_elem ' +
            'where table_name= ? AND change_time <= ? ' +
            ' order by change_time) op ' +
            'left join ' + tableName + ' c on c.' + idName + ' = op.IdOper ' +
            'where (TipoOper="U" and ' + idName + ' is not null) or TipoOper="D" ' +
            'order by change_time, TipoOper';

            this._selectSql(sql, [tableName, this.syncDate, tableName, this.syncDate], tx, dataCallBack);
        },
        _getDataToDelete: function (tableName, tx, dataCallBack) {
            var sql = 'select distinct id FROM delete_elem' +
                ' WHERE table_name = ? AND change_time <= ?' +
                ' ORDER BY change_time ';
            this._selectSql(sql, [tableName, this.syncDate], tx, dataCallBack);
        },
        _detectConflict: function(tableName, idValue, tx, callBack)  {
            var sql, self = this;
            if (!this.firstSync) {
                sql = 'select DISTINCT id FROM new_elem ' +
                    ' WHERE table_name = ?  AND id = ? AND change_time > ? ' +
                    ' UNION ALL ' +
                    'select DISTINCT id FROM delete_elem ' +
                    ' WHERE table_name = ? AND id = ? AND change_time > ?';

                self._selectSql(sql, [tableName, idValue, this.syncDate, tableName, idValue, this.syncDate], tx,
                function(exists) {
                    if (exists.length) { callBack(true); } else { callBack(false); }
                });
            } else {
            callBack(false);
            }
        },
        _updateRecord: function (tableName, idName, reg, tx, callBack) {
            var sql, self = this;

            this._detectConflict(tableName, reg[idName], tx, function (exists) {
                if (!exists) {
                    /*ex : UPDATE "tableName" SET colonne 1 = [valeur 1], colonne 2 = [valeur 2]*/
                    var attList = self._getAttributesList(tableName, reg);
                    sql = self._buildUpdateSQL(tableName, reg, attList);
                    sql += ' WHERE ' + idName + ' = ? ';
                    var attValue = self._getMembersValue(reg, attList);
                    attValue.push(reg[idName]);
                    self._executeSql(sql, attValue, tx, function() {
                        sql = 'DELETE FROM new_elem WHERE ' +
                            'table_name = ? AND id = ? AND ' +
                            'change_time = (select MAX(change_time) FROM new_elem  ' +
                            'WHERE table_name = ?  AND id = ?) ';

                        self._executeSql(sql, [tableName, reg[idName], tableName, reg[idName] ], tx,
                        function() {
                            //self.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Updated});
                            callBack();
                        },
                        function(ts, error) {
                            self._errorHandler(ts, error);
                            callBack(error);
                        });
                    });

                } else {  // send conflict to server
                callBack();
                self._sendConflict(tableName, idName, reg, tx);
                }
            });
        },
        _updateLocalDb: function(serverData, callBack) {
            var sqlErrs = [], self = this;
            var table = serverData.table;
            var currData = serverData.currData;
            var deleData = serverData.deleData;
            var counterNbElm = 0;
            var nb = currData.length;
            var nbDel = deleData.length;
            counterNbElm += nb;
            this.log('There are ' + nb.toString() + ' new or modified elements and ' + nbDel.toString() + ' deleted, in the table ' + table.tableName + ' to save in the local DB.');
            var counterNbElmTab = 0;

            var callOperation = function(err) {
              counterNbElmTab++;
              if (err) { sqlErrs.push(err); }
              if (counterNbElmTab === nb ) {
                  self._finishSync(table.tableName, self.serverData.syncDate);
              }
            };

            this.db.transaction(function(tx) {
            self._deleteTableLocalDb (table.tableName, table.idName, deleData, tx, function () {
                var listIdToCheck = [];
                if (nb !== 0) {
                for (var i = 0; i < nb; i++) {
                    listIdToCheck.push(currData[i][table.idName]);
                }
                self._getIdExitingInDB(table.tableName, table.idName, listIdToCheck, tx, function (idInDb) {
                    var curr;
                    for (var i = 0; i < nb; i++) {
                    curr = currData[i];

                    if (idInDb.indexOf(curr[table.idName]) !== -1) {// update
                        self._updateRecord(table.tableName, table.idName, curr, tx, callOperation);
                    } else {// insert
                        self._insertRecord(table.tableName, table.idName, curr, tx, callOperation);
                    }

                    } // end for
                }); // end getExisting Id
                } else  { self._finishSync(table.tableName, self.serverData.syncDate); }
            }); // end delete elements
            },function (err)  {
            self.log('TransactionError (' + table.tableName + '): ' + err.message);
            sqlErrs.push(err);
            self._errorHandler(undefined, err);
            callBack(table, 'updateLocalDb', sqlErrs);
            }, function()  {
            if (sqlErrs.length === 0) { callBack(table, 'updateLocalDb'); } else { callBack(table, 'updateLocalDb', sqlErrs); }
            }); // end tx
        },
        _updateFirstLocalDb: function(serverData, callBack) {
            var sqlErrs = [], self = this;
            var table = serverData.table;
            var currData = serverData.currData;
            var counterNbElm = 0;
            var nb = currData.length;
            counterNbElm += nb;
            this.log('There are ' + nb + ' new elements, in the table ' + table.tableName + ' to save in the local DB');

            var counterNbElmTab = 0;
            this.db.transaction ( function (tx)  {
            if (nb !== 0) {
                for (var i = 0; i < nb; i++) {
                self._insertRecord(table.tableName, table.idName, currData[i], tx,function (err)  {
                    counterNbElmTab++;
                    if (err) { sqlErrs.push(err); }
                    if (counterNbElmTab === nb) {
                    self._finishSync(table.tableName, self.serverData.syncDate);
                    }
                });
                }
            }
            },function (err)  {
            self.log('TransactionError (' + table.tableName + '): ' + err.message);
            sqlErrs.push(err);
            self._errorHandler(undefined, err);
            callBack(table, 'updateFirstLocalDb', sqlErrs);
            }, function()  {
            self.syncResult.nbUpdated += counterNbElmTab;
            if (sqlErrs.length === 0) { callBack(table, 'updateFirstLocalDb'); }  else { callBack(table, 'updateFirstLocalDb', sqlErrs); }
            });

        },
        _insertRecord: function(tableName, idName, reg, tx, callBack) {
            var sql, self = this;

            this._detectConflict(tableName, reg[idName], tx,
            function(exists)  {
                if (!exists) {

                    // 'ex INSERT INTO tablename (id, name, type, etc) VALUES (?, ?, ?, ?);'
                    var attList = self._getAttributesList(tableName, reg);
                    sql = self._buildInsertSQL(tableName, reg, attList);
                    var attValue = self._getMembersValue(reg, attList);
                    if (!self.firstSync) {
                        self._executeSql(sql, attValue, tx, () => {
                            sql = 'DELETE FROM new_elem WHERE ' +
                                'table_name = ? AND id = ? AND ' +
                                'change_time = (select MAX(change_time) FROM new_elem WHERE ' +
                                'table_name = ? AND id = ?) ';

                            self._executeSql(sql, [tableName, reg[idName], tableName, reg[idName]], tx,
                            function() {
                                //this.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Inserted});
                                callBack ();
                            });
                        }, function (ts, error)  {
                        self._errorHandler(ts, error);
                        callBack(error);
                        });
                    } else {
                        self._executeSql(sql, attValue, tx,
                        () => {
                            //this.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Inserted});
                            callBack ();
                        },
                        function(ts, error) {
                            self._errorHandler(ts, error);
                            callBack(error);
                        });
                    }
                } else {  // send conflict to server
                    self._sendConflict(tableName, idName, reg, tx);
                }
            });
        },
        _sendConflict: function(tableName, idName, reg, tx) {
            var self = this, sql;

            sql = 'select * FROM ' + tableName + ' WHERE ' + idName + ' = ?';
            self._selectSql(sql, [reg[idName]], tx, function (regloc) {
                var dataToSend = {
                info: self.syncInfo,
                client: null,
                server: null
                };
                if (regloc.length === 0) {
                    dataToSend.client = 'DELETED';
                } else {
                    dataToSend.client = regloc;
                }
                dataToSend.server = reg;
                self._sendConflictToServer(dataToSend);
            });
        },
        _transformRs: function(rs) {
            var elms = [];
            if (typeof rs.rows === 'undefined') {
                return elms;
            }

            for (var i = 0, ObjKeys; i < rs.rows.length; ++i) {
                ObjKeys = Object.keys(rs.rows.item(i));
                if (ObjKeys.length === 1 ) {
                    elms.push(rs.rows.item(i)[ ObjKeys[0] ] );
                } else {
                    elms.push(rs.rows.item(i));
                }
            }
            return elms;
        },
        _deleteTableLocalDb: function(tablename, idName, listIdToDelete, tx, callBack) {
            var listIds = [], self = this, orden = 0;
            if (listIdToDelete.length === 0) {
                callBack(true);
            } else {
                listIds.push(
                    listIdToDelete.reduce(function(listIdsDel, id, ix) {
                        if ((ix % 50) === 0 && listIdsDel.length !== 0) {
                            listIds.push(listIdsDel);
                            listIdsDel = [];
                        }
                        listIdsDel.push(id);
                        return listIdsDel;
                    }, [])
                );

                listIds.map(function(listIdsDel, ix, list) {
                  self._deleteParcialTableLocalDb(tablename, idName, listIdsDel, tx,function () {
                        if (++orden === list.length) { callBack(true); }
                    });
                });
            }
        },
        _deleteParcialTableLocalDb: function(tablename, idName, listIdToDelete, tx, callBack) {
            var self = this;

            var  sql = 'delete from ' + tablename + ' WHERE ' + idName + ' IN (' +
                listIdToDelete.map(function (x) { return '?'; }).join(',') + ')';
            this._executeSql(sql, listIdToDelete, tx, function()  {
                sql = 'delete from delete_elem WHERE table_name = "' + tablename +'" and id  IN (' +
                    listIdToDelete.map(function (x) { return '?'; }).join(',') + ')';
                self._executeSql(sql, listIdToDelete, tx, function()  {
                    var reg = {};
                    listIdToDelete.forEach( function (x) {
                        reg[idName] = x;
                        //self.dataObserver.next({table: tablename, record: reg, operation: DataOperation.Deleted});
                    });
                    callBack(true);
                });
            });
        },
        _getIdExitingInDB: function(tableName, idName, listIdToCheck, tx, dataCallBack) {
            var self = this;
            var listIds = [], idsInDb = [];
            var orden = 0;
            if (listIdToCheck.length === 0) {
                dataCallBack([]);
            } else {
                listIds.push(
                    listIdToCheck.reduce(function(listIdsCheck, id, ix)  {
                        if ((ix % 50) === 0 && listIdsCheck.length !== 0) {
                            listIds.push(listIdsCheck);
                            listIdsCheck = [];
                        }
                        listIdsCheck.push(id);
                        return listIdsCheck;
                    }, [])
                );

                listIds.map(function(listIdsCheck, ix, list) {
                  self._getParcialIdExitingInDB(tableName, idName, listIdsCheck, tx, function (idsFind) {
                        idsInDb = idsInDb.concat(idsFind);
                        if (++orden === list.length) {
                            dataCallBack(idsInDb);
                        }
                    });
                });
            }
        },
        _getParcialIdExitingInDB: function(tableName, idName, listIdToCheck, tx, dataCallBack ) {
            var sql = 'select ' + idName + ' FROM ' + tableName + ' WHERE ' + idName + ' IN (' +
                listIdToCheck.map(function (x) { return '?'; }).join(',') + ')';
            this._selectSql(sql, listIdToCheck, tx, function (idsFind)  {
                dataCallBack(idsFind);
            });
        },
        _executeSqlBridge: function(tx, sql, params, dataHandler, errorHandler) {
            // Standard WebSQL
            var self = this;
            tx.executeSql(sql, params, dataHandler,
                function(transaction, error)  {
                  self.log('sql error: ' + sql);
                  self.SqlTranError = {message: error.message, code: error.code, sql: sql};
                  return errorHandler(transaction, error);
                }
            );
        },
        _executeSql: function(sql, params, optionalTransaction, optionalCallBack, optionalErrorHandler) {
            var self = this;
            if (params) {
                this.logSql('_executeSql: ' + sql + ' with param ' + params.join(','));
            } else {
                this.logSql('_executeSql: ' + sql);
            }
            if (!optionalCallBack) {
                optionalCallBack = self._defaultCallBack;
            }
            if (!optionalErrorHandler) {
                optionalErrorHandler = this._errorHandler.bind(this);
            }
            if (optionalTransaction) {
                this._executeSqlBridge(optionalTransaction, sql, params || [], optionalCallBack, optionalErrorHandler);
            } else {
                if (sql.indexOf('select') === 0) {
                    this.db.readTransaction (function(tx) {
                        self._executeSqlBridge(tx, sql, params, optionalCallBack, optionalErrorHandler);
                    });
                } else {
                    this.db.transaction (function(tx) {
                        self._executeSqlBridge(tx, sql, params, optionalCallBack, optionalErrorHandler);
                    });
                }
            }
        },
        _defaultCallBack:function(transaction, results) {
            // DBSYNC.log('SQL Query executed. insertId: '+results.insertId+' rows.length '+results.rows.length);
        },
        _selectSql: function(sql, params, optionalTransaction, callBack) {
            var self = this;
            this._executeSql(sql, params, optionalTransaction,
                function(tx, rs)  { callBack(self._transformRs(rs)); });
        },
        _errorHandler: function (transaction, error)  {
            // this.log(error);
            this.error('Error : ' + error.message + ' (Code ' + error.code + ')' );
            return true;
        },
        _buildInsertSQL: function(tableName, objToInsert, attrList) {
            var members;
            if (attrList) { members = attrList;
            } else { members = this._getAttributesList(tableName, objToInsert);  }
            if (members.length === 0) {
                throw new Error('buildInsertSQL : Error, try to insert an empty object in the table ' + tableName);
            }
            // build INSERT INTO myTable (attName1, attName2) VALUES (?, ?) -> need to pass the values in parameters
            var sql = 'INSERT INTO ' + tableName + ' (';
            sql += members.join(',');
            sql += ') VALUES (';
            sql += members.map(function (x) { return '?'; }).join(',');
            sql += ')';
            return sql;
        },
        _buildUpdateSQL: function(tableName, objToUpdate, attrList) {
            /*ex UPDATE "nom de table" SET colonne 1 = [valeur 1], colonne 2 = [valeur 2] WHERE {condition}*/
            var members;
            var sql = 'UPDATE ' + tableName + ' SET ';
            if (attrList) { members = attrList;
            } else {
                members = this._getAttributesList(tableName, objToUpdate);
            }
            if (members.length === 0) {
                throw new Error('buildUpdateSQL : Error, try to insert an empty object in the table ' + tableName);
            }

            var nb = members.length;
            for (var i = 0; i < nb; i++) {
                sql += '"' + members[i] + '" = ?';
                if (i < nb - 1) {
                    sql += ', ';
                }
            }

            return sql;
        },
        _replaceAll: function(value, search, replacement) {
            if (typeof value === 'string') {
                return value.split(search).join(replacement);
            } else {
                return value;
            }
        },
        _getMembersValue:function(obj, members) {
            var memberArray = [];
            members.forEach( function (member) {
            memberArray.push(obj[member]);
            });
            return memberArray;
        },
        _getObjectValues: function (obj) {
          var values = [];
          for (var elem in obj) { values.push(obj[elem]); }
          return values;
        },
        _getAttributesList: function(tableName, obj, check) {
            var memberArray = [];
            var table = this._getTableToProcess(tableName);
            for (var elm in obj) {
                if (check && typeof this[elm] === 'function' && !obj.hasOwnProperty(elm)) {
                    continue;
                }
                if (table.ddl.indexOf(elm) === -1) { continue; } else { memberArray.push(elm); }
            }
            return memberArray;
        },
        _getMembersValueString: function(obj, members, separator) {
            var result = '';
            for (var i = 0; i < members.length; i++) {
                result += '"' + obj[members[i]] + '"';
                if (i < members.length - 1) {
                    result += separator;
                }
            }
            return result;
        },
        _sendDataToServer: function(dataToSync, callBack) {
            var self = this;

            var XHR = new XMLHttpRequest();
            var data = JSON.stringify(dataToSync);
            XHR.overrideMimeType('application/json;charset=UTF-8');

            if (self.username !== null && self.password !== null &&
                self.username !== undefined && self.password !== undefined ) {
                XHR.open('POST', self.serverUrl, true);
                XHR.setRequestHeader('Authorization', 'Basic ' + self._encodeBase64(self.username + ':' + self.password));
            } else {
                XHR.open('POST', self.serverUrl, true);
            }

            XHR.setRequestHeader('Content-type', 'application/json; charset=utf-8');
            XHR.onreadystatechange = function()  {
                var serverAnswer;
                if (4 === XHR.readyState) {
                    if (XHR.status === 0 && XHR.response === '') {
                        callBack({ result: 'ERROR',
                            message: 'Se ha producido un error de red.',
                            status: XHR.status,
                            sizeResponse: 0,
                            syncDate: 0,
                            data: {},
                            models: {},
                            responseText : XHR.response});
                    }
                    try {
                        serverAnswer = JSON.parse(XHR.responseText);
                    } catch (e) {
                        serverAnswer = XHR.responseText;
                    }
                    self.log('Server answered: ');
                    self.log(JSON.stringify({result: serverAnswer.result, message: serverAnswer.message}));
                    // I want only json/object as response
                    if ((XHR.status === 200) && serverAnswer instanceof Object) {
                        callBack(serverAnswer);
                    } else if (XHR.status === 500) {
                        serverAnswer = {
                            result : 'ERROR',
                            message : 'Se ha producido un error en el servidor.',
                            status : XHR.status,
                            responseText : serverAnswer
                        };
                        callBack(serverAnswer);
                    } else if (XHR.status >= 900 && XHR.status <= 999) {
                        callBack(serverAnswer);
                    }
                }
            };

            XHR.ontimeout = function() {
                var serverAnswer = {
                    result : 'ERROR',
                    message : 'Server Time Out',
                    status : XHR.status,
                    responseText : XHR.responseText,
                    data: {},
                    models: {},
                    sizeResponse: 0,
                    syncDate: 0
                };
                callBack(serverAnswer);
            };

            XHR.send(data);
        },
        _sendConflictToServer: function(dataConflic) {
            var self = this;

            var XHR = new XMLHttpRequest();
            var data = JSON.stringify(dataConflic);
            XHR.overrideMimeType('application/json;charset=UTF-8');
            XHR.timeout = 60000;
            XHR.setRequestHeader('Content-type', 'application/json; charset=utf-8');

            if (self.username !== null && self.password !== null && self.username !== undefined && self.password !== undefined ) {
            XHR.open('POST', self.serverUrl.replace('sync', 'conflict'), true);
            XHR.setRequestHeader('Authorization', 'Basic ' + self._encodeBase64(self.username + ':' + self.password));
            } else {
            XHR.open('POST', self.serverUrl.replace('sync', 'conflict'), true);
            }

            XHR.onreadystatechange =  () => {
                var serverAnswer;
                if (4 === XHR.readyState) {
                    if (XHR.status === 0 && XHR.response === '') {
                        self.log('Error de red.');
                    }
                    try {
                        serverAnswer = JSON.parse(XHR.responseText);
                    } catch (e) {
                        serverAnswer = XHR.responseText;
                    }
                    // I want only json/object as response
                    if (XHR.status === 200 && serverAnswer instanceof Object) {
                        self.log('Server conflict answered: ');
                        self.log(JSON.stringify(serverAnswer));
                    } else {
                        self.error('Server conflict error answered: ' + JSON.stringify(serverAnswer) );
                    }
                }
            };

            XHR.ontimeout = () => {
                self.log('Server conflict timeout. ');
            };

            XHR.send(data);

        },
        _columnExists: function(table, column, optionalTransaction, callback) {
            var self = this;
            self._getDDLTable(table, optionalTransaction,
            function(ddl)  {
                if (ddl.indexOf(column) === -1) { callback(false); } else {  callback(true); }
            });
        },
        _getDDLTable: function(table, optionalTransaction, callback) {
            var sql = 'select sql from sqlite_master where type=\'table\' and name=\'' + table + '\'';
            this._selectSql(sql, [], optionalTransaction, function(rs) { callback(rs[0]); });
        },
        checkModelsList: function(tableList) {
            var listToCheck = [];
            var tablesToSync = this.tablesToSync.map( function (t) { return t.tableName; });
            if (tableList instanceof Array) {
            listToCheck = tableList;
            } else {
            listToCheck = tableList.split(',');
            }
            if (listToCheck.some(function (t) { return tablesToSync.indexOf(t) === -1; } ) ) {
            throw new Error ('Any item in the list is invalid.');
            } else {
            return listToCheck;
            }
        },
        _progressRatio: function(nbIx, nbCompleted, nbPending) {
            var nbTables = this.tablesToSync.length;
            return Math.round( (nbTables - (nbCompleted + nbPending) + nbIx) / nbTables * 100);
        },
        _encodeBase64: function(input) {
            var output = '';
            // tslint:disable-next-line:one-variable-per-declaration
            var chr1, chr2, chr3, enc1, enc2, enc3, enc4;
            var i = 0;

            input = this._utf8_encode(input);

            while (i < input.length) {

                chr1 = input.charCodeAt(i++);
                chr2 = input.charCodeAt(i++);
                chr3 = input.charCodeAt(i++);
                // tslint:disable:no-bitwise
                enc1 = chr1 >> 2;
                enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
                enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
                enc4 = chr3 & 63;
                // tslint:enable:no-bitwise
                if (isNaN(chr2)) {
                    enc3 = enc4 = 64;
                } else if (isNaN(chr3)) {
                    enc4 = 64;
                }

                output = output +
                this.keyStr.charAt(enc1) + this.keyStr.charAt(enc2) +
                this.keyStr.charAt(enc3) + this.keyStr.charAt(enc4);

            }

            return output;
        },
        _utf8_encode: function(input) {
            input = input.replace(/\r\n/g, '\n');
            var utftext = '';

            for (var n = 0; n < input.length; n++) {

                var c = input.charCodeAt(n);
                // tslint:disable:no-bitwise
                if (c < 128) {
                    utftext += String.fromCharCode(c);
                } else if ((c > 127) && (c < 2048)) {
                    utftext += String.fromCharCode((c >> 6) | 192);
                    utftext += String.fromCharCode((c & 63) | 128);
                } else {
                    utftext += String.fromCharCode((c >> 12) | 224);
                    utftext += String.fromCharCode(((c >> 6) & 63) | 128);
                    utftext += String.fromCharCode((c & 63) | 128);
                }
                // tslint:enable:no-bitwise
            }

            return utftext;
        }
    };
    return DBSYNC;
}));
