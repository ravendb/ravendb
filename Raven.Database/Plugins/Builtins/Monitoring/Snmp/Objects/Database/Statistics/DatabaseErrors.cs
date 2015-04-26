// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseErrors : DatabaseScalarObjectBase
	{
		public DatabaseErrors(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "1.5.2.{0}.1.10", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			return new Integer32(database.WorkContext.Errors.Length);
		}
	}
}