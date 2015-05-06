// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexedPerSecond.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Requests
{
	public class DatabaseIndexedPerSecond : DatabaseScalarObjectBase
	{
		public DatabaseIndexedPerSecond(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.3.2", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			return new Gauge32(GetCount(database));
		}

		private static int GetCount(DocumentDatabase database)
		{
			var metricsCounters = database.WorkContext.MetricsCounters;
			return (int)metricsCounters.IndexedPerSecond.CurrentValue;
		}
	}
}