// -----------------------------------------------------------------------
//  <copyright file="DatabaseApproximateTaskCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseCurrentNumberOfItemsToIndexInSingleBatch : DatabaseScalarObjectBase<Gauge32>
	{
		public DatabaseCurrentNumberOfItemsToIndexInSingleBatch(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.1.8", index)
		{
		}

		protected override Gauge32 GetData(DocumentDatabase database)
		{
			return new Gauge32(database.WorkContext.CurrentNumberOfItemsToIndexInSingleBatch);
		}
	}
}