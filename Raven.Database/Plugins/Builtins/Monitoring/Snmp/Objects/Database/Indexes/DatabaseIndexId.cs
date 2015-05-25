// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexId.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes
{
	public class DatabaseIndexId : DatabaseIndexScalarObjectBase<Integer32>
	{
		public DatabaseIndexId(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
			: base(databaseName, indexName, landlord, databaseIndex, indexIndex, "3")
		{
		}

		protected override Integer32 GetData(DocumentDatabase database)
		{
			return new Integer32(IndexDefinition.IndexId);
		}
	}
}