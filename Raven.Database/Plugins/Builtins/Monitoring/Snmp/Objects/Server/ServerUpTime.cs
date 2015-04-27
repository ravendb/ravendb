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
			: base("1.1.2")
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