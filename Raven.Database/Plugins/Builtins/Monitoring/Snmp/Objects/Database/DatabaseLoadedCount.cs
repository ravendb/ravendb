// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database
{
	public class DatabaseLoadedCount : ScalarObjectBase<Integer32>
	{
		private readonly DatabasesLandlord databasesLandlord;

		public DatabaseLoadedCount(DatabasesLandlord databasesLandlord)
			: base("5.1.2")
		{
			this.databasesLandlord = databasesLandlord;
		}

		protected override Integer32 GetData()
		{
			return new Integer32(GetCount(databasesLandlord));
		}

		private static int GetCount(AbstractLandlord<DocumentDatabase> landlord)
		{
			return landlord.ResourcesStoresCache.Values.Count();
		}
	}
}