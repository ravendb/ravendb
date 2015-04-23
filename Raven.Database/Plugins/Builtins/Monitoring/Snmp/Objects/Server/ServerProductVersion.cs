// -----------------------------------------------------------------------
//  <copyright file="ServerProductVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerProductVersion : ScalarObject
	{
		private readonly OctetString productVersion;

		public ServerProductVersion()
			: base(new ObjectIdentifier("1.1.4"))
		{
			productVersion = new OctetString(DocumentDatabase.ProductVersion);
		}

		public override ISnmpData Data
		{
			get { return productVersion; }
			set { throw new AccessFailureException(); }
		}
	}
}