using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        private Logger _logger;

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public readonly PatchDocument Patch;

        private readonly object _idleLocker = new object();
        private Task _indexStoreTask;
        private Task _transformerStoreTask;
        public TransactionOperationsMerger TxMerger;

        public DocumentDatabase(string name, RavenConfiguration configuration, IoMetrics ioMetrics)
        {
            Name = name;
            Configuration = configuration;
            _logger = LoggerSetup.Instance.GetLogger<DocumentDatabase>(Name);
            Notifications = new DocumentsNotifications();
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this);
            TransformerStore = new TransformerStore(this);
            SqlReplicationLoader = new SqlReplicationLoader(this);
            DocumentReplicationLoader = new DocumentReplicationLoader(this);
            DocumentTombstoneCleaner = new DocumentTombstoneCleaner(this);
            SubscriptionStorage = new SubscriptionStorage(this);
            Metrics = new MetricsCountersManager();
            IoMetrics = ioMetrics;
            Patch = new PatchDocument(this);
            TxMerger = new TransactionOperationsMerger(this, DatabaseShutdown);
            HugeDocuments = new HugeDocuments(configuration.Databases.MaxCollectionSizeHugeDocuments, 
                configuration.Databases.MaxWarnSizeHugeDocuments);

        }

        public SubscriptionStorage SubscriptionStorage { get; set; }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public string ResourceName => $"db/{Name}";

        public RavenConfiguration Configuration { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public DocumentsStorage DocumentsStorage { get; private set; }

        public BundleLoader BundleLoader { get; private set; }

        public DocumentTombstoneCleaner DocumentTombstoneCleaner { get; private set; }

        public DocumentsNotifications Notifications { get; }

        public HugeDocuments HugeDocuments { get; }

        public MetricsCountersManager Metrics { get; }

        public IoMetrics IoMetrics { get;  }

        public IndexStore IndexStore { get; private set; }

        public TransformerStore TransformerStore { get; private set; }

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
            TxMerger.Start();
            _indexStoreTask = IndexStore.InitializeAsync();
            _transformerStoreTask = TransformerStore.InitializeAsync();
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

            try
            {
                _transformerStoreTask.Wait(DatabaseShutdown);
            }
            finally
            {
                _transformerStoreTask = null;
            }

            SubscriptionStorage.Initialize();
        }

        public void Dispose()
        {
            _databaseShutdown.Cancel();
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentDatabase)}");

            exceptionAggregator.Execute(() =>
            {
                TxMerger.Dispose();
            });

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

            if (_transformerStoreTask != null)
            {
                exceptionAggregator.Execute(() =>
                {
                    _transformerStoreTask.Wait(DatabaseShutdown);
                    _transformerStoreTask = null;
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
                SubscriptionStorage?.Dispose();
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

                var alertsDocument = context.ReadObject(alerts, Constants.RavenAlerts,
                    BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                DocumentsStorage.Put(context, Constants.RavenAlerts, etag, alertsDocument);
                tx.Commit();
            }
        }

        public IEnumerable<StorageEnvironment> GetAllStoragesEnvironment()
        {
            // TODO :: more storage environments ?
            yield return DocumentsStorage.Environment;
            yield return SubscriptionStorage.Environment();
            foreach (var index in IndexStore.GetIndexes())
            {
                var env = index._indexStorage.Environment();
                if (env != null)
                    yield return env;
            }
        }
    }
}