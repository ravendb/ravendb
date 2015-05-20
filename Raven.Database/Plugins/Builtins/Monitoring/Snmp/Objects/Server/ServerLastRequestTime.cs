using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Abstractions;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
	internal class ServerLastRequestTime : ScalarObjectBase
	{
		private readonly RequestManager requestManager;

		private static readonly TimeTicks Zero = new TimeTicks(0);

		public ServerLastRequestTime(RequestManager requestManager)
			: base("1.11")
		{
			this.requestManager = requestManager;
		}

		public override ISnmpData Data
		{
			get
			{
				if (requestManager.LastRequestTime.HasValue)
					return new TimeTicks(SystemTime.UtcNow - requestManager.LastRequestTime.Value);

				return Zero;
			}
			set { throw new AccessFailureException(); }
		}
	}
}