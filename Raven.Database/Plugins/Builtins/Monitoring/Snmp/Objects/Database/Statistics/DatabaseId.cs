// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics
{
	public class DatabaseId : ScalarObject
	{
		private readonly OctetString id;

		public DatabaseId(DocumentDatabase database, int index)
			: base("1.5.2.{0}.1.11", index)
		{
			id = new OctetString(database.TransactionalStorage.Id.ToString());
		}

		public override ISnmpData Data
		{
			get { return id; }
			set { throw new AccessFailureException(); }
		}
	}
}