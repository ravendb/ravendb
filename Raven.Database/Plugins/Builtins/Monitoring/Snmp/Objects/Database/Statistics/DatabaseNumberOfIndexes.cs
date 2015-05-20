// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseNumberOfIndexes : DatabaseScalarObjectBase
	{
		public DatabaseNumberOfIndexes(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.5.1", index)
		{
		}

		protected override ISnmpData GetData(DocumentDatabase database)
		{
			return new Integer32(database.IndexStorage.IndexNames.Length);
		}
	}
}