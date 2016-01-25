using System;
using System.Collections.Concurrent;
using System.IO;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public unsafe class ServerStore : IDisposable
    {
        public string DataDirectory;

        private static readonly ILog Log = LogManager.GetLogger(typeof(ServerStore));

        private StorageEnvironment _env;
        private readonly IConfigurationRoot _config;

        private UnmanagedBuffersPool _pool;
        private ConcurrentStack<RavenOperationContext> _contextPool;


        public ServerStore(IConfigurationRoot config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _config = config;
        }

        public void Initialize()
        {
            var runInMemory = _config.Get<bool>("run.in.memory");
            if (runInMemory == false)
            {
                DataDirectory = _config.Get<string>("system.path").ToFullPath();
            }
            if (Log.IsDebugEnabled)
            {
                Log.Debug("Starting to open server store for {0}", (runInMemory ? "<memory>" : DataDirectory));
            }
            var options = runInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(DataDirectory);

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
                        "Could not open server store for " + (runInMemory ? "<memory>" : DataDirectory), e);
                }
                options.Dispose();
                throw;
            }

            _pool = new UnmanagedBuffersPool("ServerStore");// 128MB should be more than big enough for the server store
            _contextPool = new ConcurrentStack<RavenOperationContext>();
        }

        public IDisposable AllocateRequestContext(out RavenOperationContext context)
        {
            if (_contextPool.TryPop(out context) == false)
                context = new RavenOperationContext(_pool);
            context.Transaction = _env.ReadTransaction();
            return new ReturnRequestContext
            {
                Store = this,
                Context = context
            };
        }

        private class ReturnRequestContext : IDisposable
        {
            public RavenOperationContext Context;
            public ServerStore Store;
            public void Dispose()
            {
                Context.Transaction?.Dispose();

                //TODO: this probably should have low memory handle
                if (Store._contextPool.Count > 25) // don't keep too much of them around
                {
                    Context.Dispose();
                    return;
                }
                Store._contextPool.Push(Context);
            }
        }
        

        public BlittableJsonReaderObject Read(RavenOperationContext ctx, string id)
        {
            var dbs = ctx.Transaction.ReadTree("items");
            var result = dbs.Read(id);
            if (result == null)
                return null;
            return new BlittableJsonReaderObject(result.Reader.Base, result.Reader.Length, ctx);
        }

        public void Write(string id, BlittableJsonDocument document)
        {
            using (var tx = _env.WriteTransaction())
            {
                var dbs = tx.ReadTree("items");

                var ptr = dbs.DirectAdd(id, document.SizeInBytes);
                document.CopyTo(ptr);

                tx.Commit();
            }
        }

        public void Dispose()
        {
            if (_contextPool != null)
            {
                RavenOperationContext result;
                while (_contextPool.TryPop(out result))
                {
                    result.Dispose();
                }
            }
            _pool?.Dispose();
            _env?.Dispose();
        }
    }
}