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
	public class ServerUrl : ScalarObjectBase
	{
		private readonly OctetString url;

		public ServerUrl(InMemoryRavenConfiguration configuration)
			: base("1.9")
		{
			url = new OctetString(configuration.ServerUrl);
		}

		public override ISnmpData Data
		{
			get { return url; }
			set { throw new AccessFailureException(); }
		}
	}
}