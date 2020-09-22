using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.Raven.Enumerators;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtl : EtlProcess<RavenEtlItem, ICommandData, RavenEtlConfiguration, RavenConnectionString>
    {
        private readonly RavenEtlConfiguration _configuration;
        private readonly ServerStore _serverStore;

        public const string RavenEtlTag = "Raven ETL";

        private RequestExecutor _requestExecutor;
        private string _recentUrl;
        public string Url => _recentUrl;

        private readonly RavenEtlDocumentTransformer.ScriptInput _script;

        public RavenEtl(Transformation transformation, RavenEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore, RavenEtlTag)
        {
            _configuration = configuration;
            _serverStore = serverStore;

            Metrics = new EtlMetricsCountersManager();

            if (configuration.TestMode == false)
            {
                _requestExecutor = CreateNewRequestExecutor(configuration, serverStore);

                _serverStore.Server.ServerCertificateChanged += OnServerCertificateChanged;
            }

            _script = new RavenEtlDocumentTransformer.ScriptInput(transformation);
        }

        public override EtlType EtlType => EtlType.Raven;

        private void OnServerCertificateChanged(object sender, EventArgs e)
        {
            // When the server certificate changes, we need to start using the new one.
            // Since the request executor has the old certificate, we will re-create it and it will pick up the new certificate.
            var newRequestExecutor = CreateNewRequestExecutor(_configuration, _serverStore);
            var oldRequestExecutor = _requestExecutor;

            Interlocked.Exchange(ref _requestExecutor, newRequestExecutor);

            oldRequestExecutor?.Dispose();
        }

        internal Action<RavenEtl> BeforeActualLoad = null;

        private static RequestExecutor CreateNewRequestExecutor(RavenEtlConfiguration configuration, ServerStore serverStore)
        {
            var certificate = serverStore.Server.Certificate.Certificate;

            if (certificate != null && configuration.UsingEncryptedCommunicationChannel() == false && configuration.AllowEtlOnNonEncryptedChannel)
            {
                // we're running on HTTPS but sending data to non encrypted server
                // let's not provide the server certificate so we won't fail on request executor's URL validation

                certificate = null;
            }

            return RequestExecutor.Create(configuration.Connection.TopologyDiscoveryUrls, configuration.Connection.Database, certificate, DocumentConventions.DefaultForServer);
        }

        protected override IEnumerator<RavenEtlItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToRavenEtlItems(docs, collection);
        }

        protected override IEnumerator<RavenEtlItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
        {
            return new TombstonesToRavenEtlItems(context, tombstones, collection, trackAttachments);
        }

        protected override IEnumerator<RavenEtlItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, List<string> collections)
        {
            return new AttachmentTombstonesToRavenEtlItems(context, tombstones, collections);
        }

        protected override IEnumerator<RavenEtlItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection)
        {
            return new CountersToRavenEtlItems(context, counters, collection);
        }
        
        protected override IEnumerator<RavenEtlItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
        {
            return new TimeSeriesToRavenEtlItems(timeSeries, collection);
        }
        protected override IEnumerator<RavenEtlItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            return new TimeSeriesDeletedRangeToRavenEtlItems(timeSeries, collection);
        }

        protected override bool ShouldTrackAttachmentTombstones()
        {
            // if script isn't empty and we have addAttachment() calls there we send DELETE doc command before sending transformation results (docs and attachments)

            return ShouldTrackAttachmentTombstones(Transformation);
        }

        public static bool ShouldTrackAttachmentTombstones(Transformation transformation)
        {
            return transformation.IsEmptyScript;
        }

        public override bool ShouldTrackCounters()
        {
            // we track counters only if script is empty (then we send all counters together with documents) or
            // when load counter behavior functions are defined, otherwise counters are send on document updates
            // when addCounter() is called during transformation

            return Transformation.IsEmptyScript || Transformation.Counters.CollectionToLoadBehaviorFunction != null;
        }

        public override bool ShouldTrackTimeSeries()
        {
            // we track timeSeries only if script is empty (then we send all timeSeries together with documents) or
            // when load timeSeries behavior functions are defined, otherwise timeSeries are send on document updates
            // when addTimeSeries() is called during transformation

            return Transformation.IsEmptyScript || Transformation.TimeSeries.CollectionToLoadBehaviorFunction != null;
        }

        protected override EtlTransformer<RavenEtlItem, ICommandData> GetTransformer(DocumentsOperationContext context)
        {
            return new RavenEtlDocumentTransformer(Transformation, Database, context, _script);
        }

        protected override int LoadInternal(IEnumerable<ICommandData> items, DocumentsOperationContext context)
        {
            var commands = items as List<ICommandData>;

            Debug.Assert(commands != null);

            if (commands.Count == 0)
                return 0;

            BatchOptions options = null;
            if (Configuration.LoadRequestTimeoutInSec != null)
            {
                options = new BatchOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(Configuration.LoadRequestTimeoutInSec.Value)
                };
            }

            var batchCommand = new SingleNodeBatchCommand(DocumentConventions.DefaultForServer, context, commands, options);

            var duration = Stopwatch.StartNew();

            try
            {
                BeforeActualLoad?.Invoke(this);

                AsyncHelpers.RunSync(() => _requestExecutor.ExecuteAsync(batchCommand, context, token: CancellationToken));
                _recentUrl = _requestExecutor.Url;

                return commands.Count;
            }
            catch (OperationCanceledException e)
            {
                if (CancellationToken.IsCancellationRequested == false)
                {
                    ThrowTimeoutException(commands.Count, duration.Elapsed, e);
                }

                throw;
            }
        }

        protected override bool ShouldFilterOutHiLoDocument()
        {
            // if we transfer all documents to the same collections (no script specified) then don't exclude HiLo docs
            return Transformation.IsEmptyScript == false;
        }

        private static void ThrowTimeoutException(int numberOfCommands, TimeSpan duration, Exception e)
        {
            var message = $"Load request applying {numberOfCommands} commands timed out after {duration}.";

            throw new TimeoutException(message, e);
        }

        public override void Dispose()
        {
            base.Dispose();

            if (_configuration.TestMode == false)
            {
                _serverStore.Server.ServerCertificateChanged -= OnServerCertificateChanged;

                _requestExecutor?.Dispose();
            }
        }
    }
}
