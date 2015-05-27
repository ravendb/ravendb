// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes
{
	public class DatabaseIndexName : DatabaseIndexScalarObjectBase<OctetString>
	{
		private readonly OctetString name;

		public DatabaseIndexName(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
			: base(databaseName, indexName, landlord, databaseIndex, indexIndex, "2")
		{
			name = new OctetString(indexName);
		}

		protected override OctetString GetData(DocumentDatabase database)
		{
			return name;
		}
	}
}