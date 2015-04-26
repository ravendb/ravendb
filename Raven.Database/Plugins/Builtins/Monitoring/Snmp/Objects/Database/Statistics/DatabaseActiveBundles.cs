// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Abstractions.Data;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseActiveBundles : DatabaseScalarObjectBase
	{
		public DatabaseActiveBundles(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "1.5.2.{0}.1.12", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			return new OctetString(database.Configuration.Settings[Constants.ActiveBundles] ?? string.Empty);
		}
	}
}