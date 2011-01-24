//-----------------------------------------------------------------------
// <copyright file="EsentBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.Backup
{
	public class EsentBackup
	{
		private readonly JET_INSTANCE instance;
		private readonly string destination;
		public event Action<string> Notify = delegate { };

		public EsentBackup(JET_INSTANCE instance, string destination)
		{
			this.instance = instance;
			this.destination = destination;
		}

		public void Execute()
		{
			Api.JetBackupInstance(instance, destination,
								  BackupGrbit.Atomic,
								  StatusCallback);

		}

		private JET_err StatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
		{
			Notify(string.Format("Esent {0} {1} {2}", snp, snt, data).Trim());
			return JET_err.Success;
		}
	}
}
