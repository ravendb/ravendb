// -----------------------------------------------------------------------
//  <copyright file="ServerTotalMemory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;

using Lextm.SharpSnmpLib;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerTotalMemory : ScalarObjectBase<Gauge32>
	{
		public ServerTotalMemory()
			: base("1.8.1")
		{
		}

		protected override Gauge32 GetData()
		{
			using (var p = Process.GetCurrentProcess())
				return new Gauge32(p.PrivateMemorySize64 / 1024L / 1024L);
		}
	}
}