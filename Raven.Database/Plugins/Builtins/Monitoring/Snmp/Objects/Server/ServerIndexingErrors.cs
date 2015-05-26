// -----------------------------------------------------------------------
//  <copyright file="ServerCpu.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerIndexingErrors : ScalarObjectBase<Gauge32>
	{
		private readonly DatabasesLandlord landlord;

		public ServerIndexingErrors(DatabasesLandlord landlord)
			: base("1.10")
		{
			this.landlord = landlord;
		}

		protected override Gauge32 GetData()
		{
			var indexingErrors = 0;
			landlord.ForAllDatabases(database =>
			{
				indexingErrors += database.WorkContext.Errors.Length;
			});

			return new Gauge32(indexingErrors);
		}
	}
}