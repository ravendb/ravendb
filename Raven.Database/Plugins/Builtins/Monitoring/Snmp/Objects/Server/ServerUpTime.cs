using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Abstractions;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	internal class ServerUpTime : ScalarObjectBase
	{
		private readonly RequestManager requestManager;

		public ServerUpTime(RequestManager requestManager)
			: base("1.2")
		{
			this.requestManager = requestManager;
		}

		public override ISnmpData Data
		{
			get { return new TimeTicks(SystemTime.UtcNow - requestManager.StartUpTime); }
			set { throw new AccessFailureException(); }
		}
	}

	internal class ServerUpTimeGlobal : ScalarObject
	{
		private readonly RequestManager requestManager;

		public ServerUpTimeGlobal(RequestManager requestManager)
			: base("1.3.6.1.2.1.1.3.0")
		{
			this.requestManager = requestManager;
		}

		public override ISnmpData Data
		{
			get { return new TimeTicks(SystemTime.UtcNow - requestManager.StartUpTime); }
			set { throw new AccessFailureException(); }
		}
	}
}