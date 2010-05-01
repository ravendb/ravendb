using System;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Backup
{
	public class EsentBackup
	{
		private readonly Instance instance;
		private readonly string destination;
		private readonly bool fullBackup;

		public EsentBackup(Instance instance, string destination, bool fullBackup)
		{
			this.instance = instance;
			this.destination = destination;
			this.fullBackup = fullBackup;
		}

		public void Execute()
		{
			Api.JetBackupInstance(instance, destination,
			                      fullBackup ? BackupGrbit.Atomic : BackupGrbit.Incremental, 
								  null);
		}
	}
}