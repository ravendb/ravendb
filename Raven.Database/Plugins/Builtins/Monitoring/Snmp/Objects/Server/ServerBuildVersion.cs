// -----------------------------------------------------------------------
//  <copyright file="ServerBuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerBuildVersion : ScalarObjectBase<OctetString>
	{
		private readonly OctetString buildVersion;

		public ServerBuildVersion()
			: base("1.3")
		{
			buildVersion = new OctetString(DocumentDatabase.BuildVersion);
		}

		protected override OctetString GetData()
		{
			return buildVersion;
		}
	}
}