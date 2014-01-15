// -----------------------------------------------------------------------
//  <copyright file="Backup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Voron.Impl.Backup
{
	public class BackupMethods
	{
		public const string Filename = "RavenDB.Voron.Backup";

		public static FullBackup Full = new FullBackup();

		public static IncrementalBackup Incremental = new IncrementalBackup();
	}
}