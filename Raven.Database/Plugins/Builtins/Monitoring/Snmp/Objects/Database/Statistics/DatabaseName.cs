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
	public class DatabaseName : ScalarObject
	{
		private readonly OctetString name;

		public DatabaseName(DocumentDatabase database, int index)
			: base("1.5.2.{0}.1.1", index)
		{
			name = new OctetString(database.Name ?? Constants.SystemDatabase);
		}

		public override ISnmpData Data
		{
			get { return name; }
			set { throw new AccessFailureException(); }
		}
	}
}