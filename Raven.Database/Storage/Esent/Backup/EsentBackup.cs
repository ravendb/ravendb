//-----------------------------------------------------------------------
// <copyright file="EsentBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Database.Backup;

namespace Raven.Storage.Esent.Backup
{
	public class EsentBackup
	{
		private readonly JET_INSTANCE instance;
		private readonly string destination;
		private readonly BackupGrbit backupOptions;
		public event Action<string,BackupStatus.BackupMessageSeverity> Notify = delegate { };

		public EsentBackup(JET_INSTANCE instance, string destination, BackupGrbit backupOptions)
		{
			this.instance = instance;
			this.destination = destination;
			this.backupOptions = backupOptions;
		}

		public void Execute()
		{
			Api.JetBackupInstance(instance, destination,
								  backupOptions,
								  StatusCallback);

		}

		private JET_err StatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
		{
			Notify(string.Format("Esent {0} {1} {2}", snp, snt, data).Trim(), BackupStatus.BackupMessageSeverity.Informational);
			return JET_err.Success;
		}
	}
}
