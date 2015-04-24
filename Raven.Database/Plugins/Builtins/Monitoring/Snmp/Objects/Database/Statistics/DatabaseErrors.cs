// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseErrors : ScalarObject
	{
		private readonly DocumentDatabase database;

		public DatabaseErrors(DocumentDatabase database, int index)
			: base("1.5.2.{0}.1.10", index)
		{
			this.database = database;
		}

		public override ISnmpData Data
		{
			get { return new Integer32(database.WorkContext.Errors.Length); }
			set { throw new AccessFailureException(); }
		}
	}
}