// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexAttempts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Abstractions;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes
{
	public class DatabaseIndexTimeSinceLastQuery : DatabaseIndexScalarObjectBase<TimeTicks>
	{
		public DatabaseIndexTimeSinceLastQuery(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
			: base(databaseName, indexName, landlord, databaseIndex, indexIndex, "11")
		{
		}

		protected override TimeTicks GetData(DocumentDatabase database)
		{
			var lastQueryTime = database.IndexStorage.GetLastQueryTime(IndexName);
			if (lastQueryTime.HasValue)
				return new TimeTicks(SystemTime.UtcNow - lastQueryTime.Value);

			return null;
		}
	}
}