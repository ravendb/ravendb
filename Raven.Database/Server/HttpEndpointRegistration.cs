using Raven.Abstractions.Logging;
using Raven.Database.Util;

namespace Raven.Database.Server
{
	public static class HttpEndpointRegistration
	{
		public static void RegisterHttpEndpointTarget()
		{
			LogManager.RegisterTarget<DatabaseMemoryTarget>();
		}
	}
}