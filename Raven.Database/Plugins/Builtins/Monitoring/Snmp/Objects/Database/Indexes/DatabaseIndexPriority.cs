// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexPriority.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes
{
	public class DatabaseIndexPriority : DatabaseIndexScalarObjectBase<OctetString>
	{
		public DatabaseIndexPriority(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
			: base(databaseName, indexName, landlord, databaseIndex, indexIndex, "4")
		{
		}

		protected override OctetString GetData(DocumentDatabase database)
		{
			var stats = GetIndexStats(database);
			return new OctetString(stats.Priority.ToString());
		}
	}
}