using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DocumentDatabase));

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public readonly PatchDocument Patch;

        private readonly object _idleLocker = new object();

        private Task _indexStoreTask;

        public DocumentDatabase(string name, RavenConfiguration configuration, MetricsScheduler metricsScheduler)
        {
            Name = name;
            Configuration = configuration;

            Notifications = new DocumentsNotifications();
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this);
            SqlReplicationLoader = new SqlReplicationLoader(this, metricsScheduler);
            DocumentReplicationLoader = new DocumentReplicationLoader(this);
            DocumentTombstoneCleaner = new DocumentTombstoneCleaner(this);

            Metrics = new MetricsCountersManager(metricsScheduler);
            Patch = new PatchDocument(this);
        }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public string ResourceName => $"db/{Name}";

        public RavenConfiguration Configuration { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public DocumentsStorage DocumentsStorage { get; private set; }

        public BundleLoader BundleLoader { get; private set; }

        public DocumentTombstoneCleaner DocumentTombstoneCleaner { get; private set; }

        public DocumentsNotifications Notifications { get; }

        public MetricsCountersManager Metrics { get; }

        public IndexStore IndexStore { get; private set; }

        public SqlReplicationLoader SqlReplicationLoader { get; private set; }

        public DocumentReplicationLoader DocumentReplicationLoader { get; private set; }


        public void Initialize()
        {
            DocumentsStorage.Initialize();
            InitializeInternal();
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            DocumentsStorage.Initialize(options);
            InitializeInternal();
        }

        private void InitializeInternal()
        {
            _indexStoreTask = IndexStore.InitializeAsync();
            SqlReplicationLoader.Initialize();
            DocumentReplicationLoader.Initialize();

            DocumentTombstoneCleaner.Initialize();
            BundleLoader = new BundleLoader(this);

            try
            {
                _indexStoreTask.Wait(DatabaseShutdown);
            }
            finally
            {
                _indexStoreTask = null;
            }
        }

        public void Dispose()
        {
            _databaseShutdown.Cancel();
            var exceptionAggregator = new ExceptionAggregator(Log, $"Could not dispose {nameof(DocumentDatabase)}");
            
            exceptionAggregator.Execute(() =>
            {
                DocumentReplicationLoader.Dispose();
            });

            if (_indexStoreTask != null)
            {
                exceptionAggregator.Execute(() =>
                {
                    _indexStoreTask.Wait(DatabaseShutdown);
                    _indexStoreTask = null;
                });
            }

            exceptionAggregator.Execute(() =>
            {
                IndexStore?.Dispose();
                IndexStore = null;
            });

            exceptionAggregator.Execute(() =>
            {
                BundleLoader?.Dispose();
                BundleLoader = null;
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentTombstoneCleaner?.Dispose();
                DocumentTombstoneCleaner = null;
            });

            exceptionAggregator.Execute(() =>
            {
                SqlReplicationLoader?.Dispose();
                SqlReplicationLoader = null;
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentsStorage?.Dispose();
                DocumentsStorage = null;
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        public void RunIdleOperations()
        {
            if (Monitor.TryEnter(_idleLocker) == false)
                return;

            try
            {
                IndexStore?.RunIdleOperations();
            }

            finally
            {
                Monitor.Exit(_idleLocker);
            }
        }

        public void AddAlert(Alert alert)
        {
            // Ignore for now, we are going to have a new implementation
            if (DateTime.UtcNow.Ticks != 0)
                return;

            if (string.IsNullOrEmpty(alert.UniqueKey))
                throw new ArgumentNullException(nameof(alert.UniqueKey), "Unique error key must be not null");

            DocumentsOperationContext context;
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var document = DocumentsStorage.Get(context, Constants.RavenAlerts);
                DynamicJsonValue alerts;
                long? etag = null;
                if (document == null)
                {
                    alerts = new DynamicJsonValue
                    {
                        [alert.UniqueKey] = new DynamicJsonValue
                        {
                            ["IsError"] = alert.IsError,
                            ["CreatedAt"] = alert.CreatedAt,
                            ["Title"] = alert.Title,
                            ["Exception"] = alert.Exception,
                            ["Message"] = alert.Message,
                            ["Observed"] = alert.Observed,
                        }
                    };
                }
                else
                {
                    etag = document.Etag;
                    var existingAlert = (BlittableJsonReaderObject)document.Data[alert.UniqueKey];
                    alerts = new DynamicJsonValue(document.Data)
                    {
                        [alert.UniqueKey] = new DynamicJsonValue
                        {
                            ["IsError"] = alert.IsError,
                            ["CreatedAt"] = alert.CreatedAt,
                            ["Title"] = alert.Title,
                            ["Exception"] = alert.Exception,
                            ["Message"] = alert.Message,
                            ["Observed"] = alert.Observed,
                            ["LastDismissedAt"] = existingAlert?["LastDismissedAt"],
                        }
                    };
                }

                var alertsDocument = context.ReadObject(alerts, Constants.RavenAlerts, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                DocumentsStorage.Put(context, Constants.RavenAlerts, etag, alertsDocument);
                tx.Commit();
            }
        }
    }
}