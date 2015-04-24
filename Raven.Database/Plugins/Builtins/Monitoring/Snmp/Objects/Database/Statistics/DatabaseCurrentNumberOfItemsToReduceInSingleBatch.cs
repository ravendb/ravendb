// -----------------------------------------------------------------------
//  <copyright file="DatabaseApproximateTaskCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseCurrentNumberOfItemsToReduceInSingleBatch : ScalarObject
	{
		private readonly DocumentDatabase database;

		public DatabaseCurrentNumberOfItemsToReduceInSingleBatch(DocumentDatabase database, int index)
			: base("1.5.2.{0}.1.9", index)
		{
			this.database = database;
		}

		public override ISnmpData Data
		{
			get { return new Gauge32(database.WorkContext.CurrentNumberOfItemsToReduceInSingleBatch); }
			set { throw new AccessFailureException(); }
		}
	}
}