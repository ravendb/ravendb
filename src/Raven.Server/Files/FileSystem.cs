using System;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Files
{
    public class FileSystem : IResourceStore
    {
        private readonly Logger _logger;

        private readonly CancellationTokenSource _fileSystemShutdown = new CancellationTokenSource();

        private readonly object _idleLocker = new object();
        private long _usages;
        private readonly ManualResetEventSlim _waitForUsagesOnDisposal = new ManualResetEventSlim(false);
        private long _lastIdleTicks = DateTime.UtcNow.Ticks;

        public void ResetIdleTime()
        {
            _lastIdleTicks = DateTime.MinValue.Ticks;
        }

        public FileSystem(string name, RavenConfiguration configuration, ServerStore serverStore)
        {
            StartTime = SystemTime.UtcNow;
            Name = name;
            ResourceName = "fs/" + name;
            Configuration = configuration;
            _logger = LoggingSource.Instance.GetLogger<FileSystem>(Name);
            FilesStorage = new FilesStorage(this);
            Metrics = new MetricsCountersManager();
            IoMetrics = serverStore?.IoMetrics ?? new IoMetrics(256, 256);
        }

        public DateTime LastIdleTime => new DateTime(_lastIdleTicks);

        public SystemTime Time = new SystemTime();

        public string Name { get; }

        public Guid FileSystemId => FilesStorage.Environment?.DbId ?? Guid.Empty;

        public string ResourceName { get; }

        public RavenConfiguration Configuration { get; }

        public CancellationToken FileSystemShutdown => _fileSystemShutdown.Token;

        public FilesStorage FilesStorage { get; private set; }

        public MetricsCountersManager Metrics { get; }

        public IoMetrics IoMetrics { get; }

        public DateTime StartTime { get; }

        public void Initialize()
        {
            try
            {
                FilesStorage.Initialize();
                InitializeInternal();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            try
            {
                FilesStorage.Initialize(options);
                InitializeInternal();
            }
            catch (Exception)
            {
                Dispose();
            }
        }

        private void InitializeInternal()
        {
        }

        public void Dispose()
        {
            _fileSystemShutdown.Cancel();
            
            // we'll wait for 1 minute to drain all the requests from the file system
            var sp = Stopwatch.StartNew();
            while (sp.ElapsedMilliseconds < 60 * 1000)
            {
                if (Interlocked.Read(ref _usages) == 0)
                    break;

                if (_waitForUsagesOnDisposal.Wait(1000))
                    _waitForUsagesOnDisposal.Reset();
            }

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(FileSystem)}");

            exceptionAggregator.Execute(() =>
            {
                FilesStorage?.Dispose();
                FilesStorage = null;
            });

            exceptionAggregator.ThrowIfNeeded();
        }
    }
}