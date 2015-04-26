// -----------------------------------------------------------------------
//  <copyright file="DatabaseStaleIndexes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseStaleIndexes : DatabaseScalarObjectBase
	{
		public DatabaseStaleIndexes(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "1.5.2.{0}.1.3", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			var count = database.IndexStorage.Indexes.Count(indexId => database.IndexStorage.IsIndexStale(indexId, database.LastCollectionEtags));
			return new Gauge32(count);
		}
	}
}