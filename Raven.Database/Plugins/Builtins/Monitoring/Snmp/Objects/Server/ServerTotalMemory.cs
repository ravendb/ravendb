// -----------------------------------------------------------------------
//  <copyright file="ServerTotalMemory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerTotalMemory : ScalarObjectBase
	{
		public ServerTotalMemory()
			: base("1.8.1")
		{
		}

		public override ISnmpData Data
		{
			get
			{
				using (var p = Process.GetCurrentProcess())
					return new Gauge32(p.PrivateMemorySize64 / 1024L / 1024L);
			}
			set { throw new AccessFailureException(); }
		}
	}
}