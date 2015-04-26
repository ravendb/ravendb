// -----------------------------------------------------------------------
//  <copyright file="DatabaseTransactionalStorageAllocatedSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Storage
{
	public class DatabaseTransactionalStorageAllocatedSize : DatabaseScalarObjectBase
	{
		public DatabaseTransactionalStorageAllocatedSize(string databaseName, DatabasesLandlord landlord, string dots, int index)
			: base(databaseName, landlord, "1.5.2.{0}.2.1", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			var transactionalStorageSizeOnDisk = database.GetTransactionalStorageSizeOnDisk();
			return new Gauge32(transactionalStorageSizeOnDisk.AllocatedSizeInBytes / 1024L / 1024L);
		}
	}
}