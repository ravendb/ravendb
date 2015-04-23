// -----------------------------------------------------------------------
//  <copyright file="ServerName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Database.Config;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerName : ScalarObject
	{
		private readonly OctetString name;

		public ServerName(InMemoryRavenConfiguration configuration)
			: base(new ObjectIdentifier("1.1.1"))
		{
			name = new OctetString(configuration.ServerUrl);
		}

		public override ISnmpData Data
		{
			get { return name; }
			set { throw new AccessFailureException(); }
		}
	}
}