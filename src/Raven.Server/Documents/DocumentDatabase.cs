using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Document;
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
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DocumentDatabase));

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();
        public readonly PatchDocument Patch;

        private HttpJsonRequestFactory _httpJsonRequestFactory;

        private readonly object _idleLocker = new object();
        private Task _indexStoreTask;
        private Task _transformerStoreTask;
        public bool LazyTransactionMode { get; set; }
        public DateTime LazyTransactionExpiration { get; set; }
        public TransactionOperationsMerger TxMerger;
        private DocumentConvention _convention;

        public string Url { get; private set; }

        public DocumentDatabase(string name, RavenConfiguration configuration, MetricsScheduler metricsScheduler,
            LoggerSetup loggerSetup)
        {
            Name = name;
            Configuration = configuration;
            LoggerSetup = loggerSetup;

            var hostName = Dns.GetHostName();
            Url = Configuration.Core.ServerUrl.ToLower()
                                              .Replace("localhost", hostName)
                                              .Replace("127.0.0.1", hostName);

            Notifications = new DocumentsNotifications();
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this);
            TransformerStore = new TransformerStore(this);
            SqlReplicationLoader = new SqlReplicationLoader(this, metricsScheduler);
            DocumentReplicationLoader = new DocumentReplicationLoader(this);
            DocumentTombstoneCleaner = new DocumentTombstoneCleaner(this);
            SubscriptionStorage = new SubscriptionStorage(this, metricsScheduler);
            Metrics = new MetricsCountersManager(metricsScheduler);
            Patch = new PatchDocument(this);
            TxMerger = new TransactionOperationsMerger(this,DatabaseShutdown);
        }

        public SubscriptionStorage SubscriptionStorage { get; set; }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public string ResourceName => $"db/{Name}";

        public RavenConfiguration Configuration { get; }
        public LoggerSetup LoggerSetup { get; set; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public DocumentsStorage DocumentsStorage { get; private set; }

        public BundleLoader BundleLoader { get; private set; }

        public DocumentTombstoneCleaner DocumentTombstoneCleaner { get; private set; }

        public DocumentsNotifications Notifications { get; }

        public MetricsCountersManager Metrics { get; }

        public IndexStore IndexStore { get; private set; }

        public TransformerStore TransformerStore { get; private set; }

        public SqlReplicationLoader SqlReplicationLoader { get; private set; }

        public DocumentReplicationLoader DocumentReplicationLoader { get; private set; }

        public HttpJsonRequestFactory HttpRequestFactory => _httpJsonRequestFactory;

        public void Initialize(HttpJsonRequestFactory httpRequestFactory, DocumentConvention convention)
        {
            _convention = convention;
            _httpJsonRequestFactory = httpRequestFactory;
            DocumentsStorage.Initialize();
            InitializeInternal();
        }

        public void Initialize(StorageEnvironmentOptions options, HttpJsonRequestFactory httpRequestFactory, DocumentConvention convention)
        {
            _httpJsonRequestFactory = httpRequestFactory;
            _convention = convention;
            DocumentsStorage.Initialize(options);
            InitializeInternal();
        }

        public TcpConnectionInfo GetTcpInfo(string url, string apiKey)
        {
            using (var request = HttpRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, string.Format("{0}/info/tcp",
                MultiDatabase.GetRootDatabaseUrl(url)),
                HttpMethod.Get,
                new OperationCredentials(apiKey, CredentialCache.DefaultCredentials),_convention)))
            {
                var result = request.ReadResponseJson();
                return _convention.CreateSerializer().Deserialize<TcpConnectionInfo>(new RavenJTokenReader(result));
            }
        }

        private void InitializeInternal()
        {
            TxMerger.Start();
            _indexStoreTask = IndexStore.InitializeAsync();
            _transformerStoreTask = TransformerStore.InitializeAsync();
            SqlReplicationLoader.Initialize();

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
            var exceptionAggregator = new ExceptionAggregator(Log, $"Could not dispose {nameof(DocumentDatabase)}");

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

    }
}