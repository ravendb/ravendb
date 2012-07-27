using System;
using Xunit;

using ServiceStack.Redis;

namespace Raven.Bundles.Tests.IndexReplicationToRedis
{
	[CLSCompliant(false)]
	public class FactIfRedisDefaultIsAvailable : FactAttribute
	{
		public const string DefaultRedisServer = "localhost";
		public const int DefaultRedisServerPort = 6379;

		public FactIfRedisDefaultIsAvailable()
		{
			var existServer = false;

			try
			{
				using (var redisClient = new RedisClient(DefaultRedisServer, DefaultRedisServerPort))
				{
					existServer = !String.IsNullOrEmpty(redisClient.ServerVersion);
				}
			}
			catch { }

			if (!existServer)
			{
				base.Skip = String.Format("Could not find the Redis server on {0} (port:{1})", DefaultRedisServer, DefaultRedisServerPort);
				return;
			}

		}


	}
}