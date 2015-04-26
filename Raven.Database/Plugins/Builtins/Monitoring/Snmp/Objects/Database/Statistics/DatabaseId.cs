// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseId : DatabaseScalarObjectBase
	{
		private OctetString id;

		public DatabaseId(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "1.5.2.{0}.1.11", index)
		{

		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			return id ?? (id = new OctetString(database.TransactionalStorage.Id.ToString()));
		}
	}
}