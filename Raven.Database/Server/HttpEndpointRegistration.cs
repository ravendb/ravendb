using Raven.Abstractions.Logging;
using Raven.Database.Server.Connections;
using Raven.Database.Util;

namespace Raven.Database.Server
{
	public static class HttpEndpointRegistration
	{
		public static void RegisterHttpEndpointTarget()
		{
			LogManager.RegisterTarget<DatabaseMemoryTarget>();
		}

        public static void RegisterOnDemandLogTarget()
        {
            LogManager.RegisterTarget<OnDemandLogTarget>();
        }
	}
}