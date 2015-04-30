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
	public class ServerName : ScalarObjectBase
	{
		private readonly OctetString name;

		public ServerName(InMemoryRavenConfiguration configuration)
			: base("1.1")
		{
			name = new OctetString(configuration.ServerName ?? string.Empty);
		}

		public override ISnmpData Data
		{
			get { return name; }
			set { throw new AccessFailureException(); }
		}
	}
}