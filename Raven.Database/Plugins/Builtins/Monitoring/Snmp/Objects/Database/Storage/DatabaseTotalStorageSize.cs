// -----------------------------------------------------------------------
//  <copyright file="DatabaseTransactionalStorageAllocatedSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Storage
{
	public class DatabaseTotalStorageSize : ScalarObject
	{
		private readonly DocumentDatabase database;

		public DatabaseTotalStorageSize(DocumentDatabase database, int index)
			: base("1.5.2.{0}.2.4", index)
		{
			this.database = database;
		}

		public override ISnmpData Data
		{
			get { return new Gauge32(GetCount()); }
			set { throw new AccessFailureException(); }
		}

		private long GetCount()
		{
			var indexStorageSizeOnDisk = database.GetIndexStorageSizeOnDisk();
			var transactionalStorageSizeOnDisk = database.GetTransactionalStorageSizeOnDisk();
			return (indexStorageSizeOnDisk + transactionalStorageSizeOnDisk.AllocatedSizeInBytes) / 1024L / 1024L;
		}
	}
}