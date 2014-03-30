// -----------------------------------------------------------------------
//  <copyright file="MaintenanceActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class MaintenanceActions : ActionsBase
    {
        public MaintenanceActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

        public static void Restore(RavenConfiguration configuration, RestoreRequest restoreRequest, Action<string> output)
        {
            var databaseDocumentPath = Path.Combine(restoreRequest.BackupLocation, "Database.Document");
            if (File.Exists(databaseDocumentPath) == false)
            {
                throw new InvalidOperationException("Cannot restore when the Database.Document file is missing in the backup folder: " + restoreRequest.BackupLocation);
            }

            var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
            var databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();

            string storage;
            if (databaseDocument.Settings.TryGetValue("Raven/StorageTypeName", out storage) == false)
            {
                storage = "esent";
            }

            if (!string.IsNullOrWhiteSpace(restoreRequest.DatabaseLocation))
            {
                configuration.DataDirectory = restoreRequest.DatabaseLocation;
            }

            using (var transactionalStorage = configuration.CreateTransactionalStorage(storage, () => { }))
            {
                transactionalStorage.Restore(restoreRequest, output);
            }
        }

        public void StartBackup(string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument databaseDocument)
        {
            if (databaseDocument == null) throw new ArgumentNullException("databaseDocument");
            var document = Database.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
            if (document != null)
            {
                var backupStatus = document.DataAsJson.JsonDeserialization<BackupStatus>();
                if (backupStatus.IsRunning)
                {
                    throw new InvalidOperationException("Backup is already running");
                }
            }

            bool enableIncrementalBackup;
            if (incrementalBackup &&
                TransactionalStorage is Raven.Storage.Esent.TransactionalStorage &&
                (bool.TryParse(Database.Configuration.Settings["Raven/Esent/CircularLog"], out enableIncrementalBackup) == false || enableIncrementalBackup))
            {
                throw new InvalidOperationException("In order to run incremental backups using Esent you must have circular logging disabled");
            }

            if (incrementalBackup &&
                TransactionalStorage is Raven.Storage.Voron.TransactionalStorage &&
                bool.TryParse(Database.Configuration.Settings["Raven/Voron/AllowIncrementalBackups"], out enableIncrementalBackup) == false)
            {
                throw new InvalidOperationException("In order to run incremental backups using Voron you must have the appropriate setting key (Raven/Voron/AllowIncrementalBackups) set to true");
            }

            Database.Documents.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(new BackupStatus
            {
                Started = SystemTime.UtcNow,
                IsRunning = true,
            }), new RavenJObject(), null);
            Database.IndexStorage.FlushMapIndexes();
            Database.IndexStorage.FlushReduceIndexes();

            if (databaseDocument.Settings.ContainsKey("Raven/StorageTypeName") == false)
                databaseDocument.Settings["Raven/StorageTypeName"] = TransactionalStorage.FriendlyName ?? TransactionalStorage.GetType().AssemblyQualifiedName;

            TransactionalStorage.StartBackupOperation(Database, backupDestinationDirectory, incrementalBackup, databaseDocument);
        }
    }
}