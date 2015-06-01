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
	public class DatabaseStaleIndexes : DatabaseScalarObjectBase<Gauge32>
	{
		public DatabaseStaleIndexes(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.1.3", index)
		{
		}

		protected override Gauge32 GetData(DocumentDatabase database)
		{
			var count = database.IndexStorage.Indexes.Count(indexId => database.IndexStorage.IsIndexStale(indexId, database.LastCollectionEtags));
			return new Gauge32(count);
		}
	}
}