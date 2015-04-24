// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Abstractions.Data;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseActiveBundles : ScalarObject
	{
		private readonly DocumentDatabase database;

		public DatabaseActiveBundles(DocumentDatabase database, int index)
			: base("1.5.2.{0}.1.12", index)
		{
			this.database = database;
		}

		public override ISnmpData Data
		{
			get { return new OctetString(database.Configuration.Settings[Constants.ActiveBundles] ?? string.Empty); }
			set { throw new AccessFailureException(); }
		}
	}
}