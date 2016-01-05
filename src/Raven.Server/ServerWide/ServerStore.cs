using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Utils;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public class ServerStore : IDisposable
    {
        private static readonly ILog Log= LogManager.GetLogger(typeof (ServerStore));

        private StorageEnvironment _env;
        private IConfigurationRoot _config;

        public ServerStore(IConfigurationRoot config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _config = config;
        }

        public void Initialize()
        {
            var runInMemory = _config.Get<bool>("run.in.memory");
            var path = _config.Get<string>("system.path").ToFullPath();
            if (Log.IsDebugEnabled)
            {
                Log.Debug("Starting to open server store for {0}", (runInMemory ? "<memory>" : path));
            }
            var options = runInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(path);

            options.SchemaVersion = 1;

            try
            {
                _env = new StorageEnvironment(options);
                using (var tx = _env.WriteTransaction())
                {
                    tx.CreateTree("items");
                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.FatalException(
                        "Could not open server store for " + (runInMemory ? "<memory>" : path), e);
                }
                options.Dispose();
                throw;
            }
        }

        // TODO: store as blittable, so no alloc for this
        public RavenJObject Read(string id)
        {
            using (var tx = _env.ReadTransaction())
            {
                var dbs = tx.ReadTree("items");
                var result = dbs.Read(id);
                if (result == null)
                    return null;
                using (var asStream = result.Reader.AsStream())
                {
                    var db = RavenJObject.Load(new JsonTextReader(new StreamReader(asStream)));
                    return db;
                }
            }
        }

        public void Write(string id, RavenJObject obj)
        {
            using (var tx = _env.WriteTransaction())
            {
                var dbs = tx.ReadTree("items");
                var ms = new MemoryStream();
                var streamWriter = new StreamWriter(ms);
                obj.WriteTo(new JsonTextWriter(streamWriter));
                streamWriter.Flush();
                ms.Position = 0;
                dbs.Add(id, ms);

                tx.Commit();
            }
        }

        public void Dispose()
        {
            _env?.Dispose();
        }
    }
}