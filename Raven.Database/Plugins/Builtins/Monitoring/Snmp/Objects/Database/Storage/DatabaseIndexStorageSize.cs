// -----------------------------------------------------------------------
//  <copyright file="DatabaseTransactionalStorageAllocatedSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Storage
{
	public class DatabaseIndexStorageSize : DatabaseScalarObjectBase
	{
		public DatabaseIndexStorageSize(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "1.5.2.{0}.2.3", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			var indexStorageSizeOnDisk = database.GetIndexStorageSizeOnDisk();
			return new Gauge32(indexStorageSizeOnDisk / 1024L / 1024L);
		}
	}
}