// -----------------------------------------------------------------------
//  <copyright file="DatabaseTransactionalStorageDiskRemainingSpace.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Storage
{
	public class DatabaseTransactionalStorageDiskRemainingSpace : DatabaseScalarObjectBase<Gauge32>
	{
		private static readonly Gauge32 Empty = new Gauge32(-1);

		public DatabaseTransactionalStorageDiskRemainingSpace(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.2.5", index)
		{
		}

		protected override Gauge32 GetData(DocumentDatabase database)
		{
			if (database.Configuration.RunInMemory) 
				return Empty;
			
			var result = CheckFreeDiskSpace.DiskSpaceChecker.GetFreeDiskSpace(database.Configuration.DataDirectory, DriveInfo.GetDrives());
			if (result == null) 
				return Empty;

			return new Gauge32(result.TotalFreeSpaceInBytes / 1024L / 1024L);
		}
	}
}