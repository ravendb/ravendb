using System;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Voron;

namespace SampleStartups.ServerWide
{
    public class ServerStore : IDisposable
    {
        private static readonly ILog Log= LogManager.GetLogger(typeof (ServerStore));

        private readonly ServerConfig _config;

        private StorageEnvironment _env;

        public ServerStore(ServerConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _config = config;
        }

        public void Initialize()
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug("Starting to open server store for {0}", (_config.RunInMemory ? "<memory>" : _config.Path));
            }
            var options = _config.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(_config.Path);

            options.SchemaVersion = 1;

            try
            {
                _env = new StorageEnvironment(options);
            }
            catch (Exception e)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.FatalException(
                        "Could not open server store for " + (_config.RunInMemory ? "<memory>" : _config.Path), e);
                }
                options.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            _env?.Dispose();
        }
    }
}