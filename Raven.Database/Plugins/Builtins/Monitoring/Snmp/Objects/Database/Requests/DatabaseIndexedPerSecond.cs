// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Requests
{
	public class DatabaseIndexedPerSecond : ScalarObject
	{
		private readonly DocumentDatabase database;

		public DatabaseIndexedPerSecond(DocumentDatabase database, int index)
			: base("1.5.2.{0}.3.2", index)
		{
			this.database = database;
		}

		public override ISnmpData Data
		{
			get { return new Gauge32(GetCount()); }
			set { throw new AccessFailureException(); }
		}

		private int GetCount()
		{
			var metricsCounters = database.WorkContext.MetricsCounters;
			return (int)metricsCounters.IndexedPerSecond.CurrentValue;
		}
	}
}