// -----------------------------------------------------------------------
//  <copyright file="ServerProductVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerProductVersion : ScalarObjectBase<OctetString>
	{
		private readonly OctetString productVersion;

		public ServerProductVersion()
			: base("1.4")
		{
			productVersion = new OctetString(DocumentDatabase.ProductVersion);
		}

		protected override OctetString GetData()
		{
			return productVersion;
		}
	}
}