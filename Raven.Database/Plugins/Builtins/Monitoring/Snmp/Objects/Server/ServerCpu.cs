// -----------------------------------------------------------------------
//  <copyright file="ServerCpu.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Database.Config;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerCpu : ScalarObjectBase
	{
		public ServerCpu()
			: base("1.1.7")
		{
		}

		public override ISnmpData Data
		{
			get { return new Gauge32((int)CpuStatistics.Average); }
			set { throw new AccessFailureException(); }
		}
	}
}