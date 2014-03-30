//-----------------------------------------------------------------------
// <copyright file="BackupOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Backup;
using Raven.Json.Linq;
using Raven.Storage.Esent;
using Raven.Storage.Esent.Backup;

namespace Raven.Database.Storage.Esent.Backup
{
	public class BackupOperation : BaseBackupOperation
	{
		private readonly JET_INSTANCE instance;
	    private string backupConfigPath;

        public BackupOperation(DocumentDatabase database, string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup,
	                           DatabaseDocument databaseDocument)
            : base(database, backupSourceDirectory, backupDestinationDirectory, incrementalBackup, databaseDocument)
	    {
	        instance = ((TransactionalStorage) database.TransactionalStorage).Instance;
            backupConfigPath = Path.Combine(backupDestinationDirectory, "RavenDB.Backup");
	    }

        protected override bool BackupAlreadyExists
        {
            get { return Directory.Exists(backupDestinationDirectory) && File.Exists(backupConfigPath); }
        }

        protected override void ExecuteBackup(string backupPath, bool isIncrementalBackup)
        {
            if (string.IsNullOrWhiteSpace(backupPath)) throw new ArgumentNullException("backupPath");

            // It doesn't seem to be possible to get the % complete from an esent backup, but any status msgs 
            // that is does give us are displayed live during the backup.
            var esentBackup = new EsentBackup(instance, backupPath, isIncrementalBackup ? BackupGrbit.Incremental : BackupGrbit.Atomic);
            esentBackup.Notify += UpdateBackupStatus;
            esentBackup.Execute();
        }

        protected override void OperationFinished()
        {
            base.OperationFinished();

            File.WriteAllText(backupConfigPath, "Backup completed " + SystemTime.UtcNow);
        }
	}
}
