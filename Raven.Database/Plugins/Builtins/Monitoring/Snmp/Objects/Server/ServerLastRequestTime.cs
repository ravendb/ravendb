using Lextm.SharpSnmpLib;

using Raven.Abstractions;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	internal class ServerLastRequestTime : ScalarObjectBase<TimeTicks>
	{
		private readonly RequestManager requestManager;

		public ServerLastRequestTime(RequestManager requestManager)
			: base("1.11")
		{
			this.requestManager = requestManager;
		}

		protected override TimeTicks GetData()
		{
			if (requestManager.LastRequestTime.HasValue)
				return new TimeTicks(SystemTime.UtcNow - requestManager.LastRequestTime.Value);

			return null;
		}
	}
}