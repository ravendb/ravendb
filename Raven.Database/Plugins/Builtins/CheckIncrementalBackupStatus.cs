// -----------------------------------------------------------------------
//  <copyright file="CheckIncrementalBackupStatus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
    public class CheckIncrementalBackupStatus : IServerStartupTask
    {
        private readonly TimeSpan frequency = TimeSpan.FromMinutes(30);

        private RavenDBOptions options;

        public void Execute(RavenDBOptions serverOptions)
        {
            options = serverOptions;
            options.SystemDatabase.TimerManager.NewTimer(ExecuteCheck, TimeSpan.Zero, frequency);
        }

        private void ExecuteCheck(object state)
        {
            var databaseLandLord = options.DatabaseLandlord;
            var systemDatabase = databaseLandLord.SystemDatabase;
            if (options.Disposed)
            {
                Dispose();
                return;
            }

            int nextStart = 0;
            var databases = systemDatabase.Documents
                .GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, 0, int.MaxValue, systemDatabase.WorkContext.CancellationToken, ref nextStart);

            foreach (var database in databases)
            {
                var db = database.JsonDeserialization<DatabaseDocument>();
                var dbName = database.Value<RavenJObject>(Constants.Metadata).Value<string>("@id").Split('/').Last();
                int alertTimeout;
                if (!(db.Settings.ContainsKey(Constants.IncrementalBackupAlertTimeout) && int.TryParse(db.Settings[Constants.IncrementalBackupAlertTimeout], out alertTimeout)))
                {
                    alertTimeout = 24;
                }
                int recurringAlertTimeout;
                if (!(db.Settings.ContainsKey(Constants.IncrementalBackupRecurringAlertTimeout) && int.TryParse(db.Settings[Constants.IncrementalBackupRecurringAlertTimeout], out recurringAlertTimeout)))
                {
                    recurringAlertTimeout = 7;
                }
                if (!IsIncrementalBackupIsAllowed(databaseLandLord,db)) continue;

                var dbStatusKey =  "Raven/BackupStatus/" +dbName;

                var incrementalBackupStatus = systemDatabase.Documents.Get(dbStatusKey);
                if (incrementalBackupStatus == null) continue;
                var dbStatus = incrementalBackupStatus.DataAsJson.JsonDeserialization<DatabaseOperationsStatus>();
                var now = SystemTime.UtcNow;

                var dbFolder = database.Value<RavenJObject>("Settings").Value<string>("Raven/DataDir");
                FileInfo fi;
                if (GetStorageFileInfo(dbFolder, out fi))
                {
                    // If the db didn't change in the past alertTimeout we shouldn't raise an alert.
                    if (now - fi.LastWriteTimeUtc > TimeSpan.FromHours(alertTimeout)) continue;
                }
                //the alertTimeout is not over so we shouldn't issue any alerts.
                if (dbStatus.LastBackup.HasValue && (now - dbStatus.LastBackup.Value) < TimeSpan.FromHours(alertTimeout)) continue;
                //we already issued an alet in the past recurringAlertTimeout so we won't issue a new one.
                if (dbStatus.LastAlertIssued.HasValue && (now - dbStatus.LastAlertIssued.Value) < TimeSpan.FromDays(recurringAlertTimeout)) continue;
                //if we are here we need to issue an alert and update the last alert time to now.
                systemDatabase.AddAlert(new Alert
                {
                    Message = String.Format("{0} was not backed up since {1}.\nThis prevents RavenDB from releasing diskspace!", dbName, dbStatus.LastBackup.Value),
                    CreatedAt = now,
                    Title = String.Format("Incremental backup warning ({0}) ",dbName),
                    UniqueKey = dbName + "/IncrementalBackupAlert"
                });
                dbStatus.LastAlertIssued = now;
                var json = RavenJObject.FromObject(dbStatus);
                //updating last time a warning was issued.
                systemDatabase.Documents.Put(dbStatusKey, null, json, new RavenJObject(), null);
            }	
        }

        private static bool GetStorageFileInfo(string dbFolder,out FileInfo fi)
        {
            var folder = dbFolder.Replace("~", "Database");
            var fileName = Path.Combine(folder, "Data");
            if (File.Exists(fileName))
            {
                fi = new FileInfo(fileName);
                return true;
            }
            else
            {
                fileName = Path.Combine(folder, "Raven.voron");
                if (File.Exists(fileName))
                {
                    fi = new FileInfo(fileName);
                    return true;
                }
            }
            fi = null;
            return false;
        }

        private static bool IsIncrementalBackupIsAllowed(DatabasesLandlord databaseLandlord, DatabaseDocument dbDoc)
        {
            // check if DatabaseDocument contains the incremental flag
            var isIncrementalBackup = dbDoc.Settings.ContainsKey(RavenConfiguration.GetKey(x => x.Storage.AllowIncrementalBackups));

            if (isIncrementalBackup && bool.TryParse(dbDoc.Settings[RavenConfiguration.GetKey(x => x.Storage.AllowIncrementalBackups)], out isIncrementalBackup))
            {
                return isIncrementalBackup;
            }

            // if not check system configuration
            return databaseLandlord.SystemConfiguration.Storage.AllowIncrementalBackups;
        }
        public void Dispose()
        {
        }
    }
} 
