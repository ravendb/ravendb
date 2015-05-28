using Lextm.SharpSnmpLib;

using Raven.Abstractions;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	internal class ServerUpTime : ScalarObjectBase<TimeTicks>
	{
		private readonly RequestManager requestManager;

		public ServerUpTime(RequestManager requestManager)
			: base("1.2")
		{
			this.requestManager = requestManager;
		}

		protected override TimeTicks GetData()
		{
			return new TimeTicks(SystemTime.UtcNow - requestManager.StartUpTime);
		}
	}

	internal class ServerUpTimeGlobal : ScalarObjectBase<TimeTicks>
	{
		private readonly RequestManager requestManager;

		public ServerUpTimeGlobal(RequestManager requestManager)
			: base("1.3.6.1.2.1.1.3.0")
		{
			this.requestManager = requestManager;
		}

		protected override TimeTicks GetData()
		{
			return new TimeTicks(SystemTime.UtcNow - requestManager.StartUpTime);
		}
	}
}