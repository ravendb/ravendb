using System;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Backup
{
	public class EsentBackup
	{
		private readonly JET_INSTANCE instance;
		private readonly string destination;
		private readonly bool fullBackup;

		public EsentBackup(JET_INSTANCE instance, string destination, bool fullBackup)
		{
			this.instance = instance;
			this.destination = destination;
			this.fullBackup = fullBackup;
		}

		public void Execute()
		{
			Api.JetBackupInstance(instance, destination,
			                      fullBackup ? BackupGrbit.Atomic : BackupGrbit.Incremental, 
								  StatusCallback);
		}

		private static JET_err StatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
		{
			Console.WriteLine("{0} {1} {2}", snp, snt, data);
			return JET_err.Success;
		}
	}
}