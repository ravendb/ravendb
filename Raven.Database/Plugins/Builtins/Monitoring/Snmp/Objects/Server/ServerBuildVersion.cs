// -----------------------------------------------------------------------
//  <copyright file="ServerBuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerBuildVersion : ScalarObjectBase
	{
		private readonly OctetString buildVersion;

		public ServerBuildVersion()
			: base("1.3")
		{
			buildVersion = new OctetString(DocumentDatabase.BuildVersion);
		}

		public override ISnmpData Data
		{
			get { return buildVersion; }
			set { throw new AccessFailureException(); }
		}
	}
}