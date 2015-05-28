// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseNumberOfAutoIndexes : DatabaseScalarObjectBase<Integer32>
	{
		public DatabaseNumberOfAutoIndexes(string databaseName, DatabasesLandlord landlord, int index)
			: base(databaseName, landlord, "5.2.{0}.5.3", index)
		{
		}

		protected override Integer32 GetData(DocumentDatabase database)
		{
			return new Integer32(database.IndexStorage.IndexNames.Count(x => x.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase)));
		}
	}
}