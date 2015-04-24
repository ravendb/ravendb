// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database
{
	public class DatabaseOpenedCount : ScalarObject
	{
		private readonly DatabasesLandlord databasesLandlord;

		public DatabaseOpenedCount(DatabasesLandlord databasesLandlord)
			: base(new ObjectIdentifier("1.5.1.2"))
		{
			this.databasesLandlord = databasesLandlord;
		}

		public override ISnmpData Data
		{
			get { return new Integer32(GetCount(databasesLandlord)); }
			set { throw new AccessFailureException(); }
		}

		private static int GetCount(AbstractLandlord<DocumentDatabase> landlord)
		{
			return landlord.ResourcesStoresCache.Values.Count();
		}
	}
}