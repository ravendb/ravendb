// -----------------------------------------------------------------------
//  <copyright file="ServerPid.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	public class ServerPid : ScalarObject
	{
		private readonly Integer32 pid;

		public ServerPid()
			: base(new ObjectIdentifier("1.1.5"))
		{
			pid = new Integer32(Process.GetCurrentProcess().Id);
		}

		public override ISnmpData Data
		{
			get { return pid; }
			set { throw new AccessFailureException(); }
		}
	}
}