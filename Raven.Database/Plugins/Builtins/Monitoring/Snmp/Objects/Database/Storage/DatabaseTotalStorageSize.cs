// -----------------------------------------------------------------------
//  <copyright file="DatabaseTransactionalStorageAllocatedSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Storage
{
	public class DatabaseTotalStorageSize : DatabaseScalarObjectBase
	{
		public DatabaseTotalStorageSize(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "1.5.2.{0}.2.4", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			var indexStorageSizeOnDisk = database.GetIndexStorageSizeOnDisk();
			var transactionalStorageSizeOnDisk = database.GetTransactionalStorageSizeOnDisk();
			return new Gauge32((indexStorageSizeOnDisk + transactionalStorageSizeOnDisk.AllocatedSizeInBytes) / 1024L / 1024L);
		}
	}
}