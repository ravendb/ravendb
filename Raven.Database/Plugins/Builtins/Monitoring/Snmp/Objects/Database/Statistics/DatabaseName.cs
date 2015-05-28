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
	public class DatabaseName : DatabaseScalarObjectBase<OctetString>
	{
		private readonly OctetString name;

		public DatabaseName(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.1.1", index)
		{
			name = new OctetString(databaseName ?? Constants.SystemDatabase);
		}

		protected override OctetString GetData(DocumentDatabase database)
		{
			return name;
		}
	}
}