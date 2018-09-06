using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.Raven.Enumerators;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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
            _requestExecutor = CreateNewRequestExecutor(configuration, serverStore);

            serverStore.Server.ServerCertificateChanged += OnServerCertificateChanged;

            _script = new RavenEtlDocumentTransformer.ScriptInput(transformation);

        }

        private void OnServerCertificateChanged(object sender, EventArgs e)
        {
            // When the server certificate changes, we need to start using the new one.
            // Since the request executor has the old certificate, we will re-create it and it will pick up the new certificate.
            var newRequestExecutor = CreateNewRequestExecutor(_configuration, _serverStore);
            var oldRequestExecutor = _requestExecutor;

            Interlocked.Exchange(ref _requestExecutor, newRequestExecutor);

            oldRequestExecutor?.Dispose();
        }

        private static RequestExecutor CreateNewRequestExecutor(RavenEtlConfiguration configuration, ServerStore serverStore)
        {
            return RequestExecutor.Create(configuration.Connection.TopologyDiscoveryUrls, configuration.Connection.Database, serverStore.Server.Certificate.Certificate, DocumentConventions.Default);
        }

        protected override IEnumerator<RavenEtlItem> ConvertDocsEnumerator(IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToRavenEtlItems(docs, collection);
        }

        protected override IEnumerator<RavenEtlItem> ConvertTombstonesEnumerator(IEnumerator<Tombstone> tombstones, string collection)
        {
            return new TombstonesToRavenEtlItems(tombstones, collection);
        }

        protected override bool ShouldTrackAttachmentTombstones()
        {
            // we send attachments of docs only if script isn't defined

            return string.IsNullOrEmpty(Transformation.Script);
        }

        protected override EtlTransformer<RavenEtlItem, ICommandData> GetTransformer(DocumentsOperationContext context)
        {
            return new RavenEtlDocumentTransformer(Database, context, _script);
        }

        protected override void LoadInternal(IEnumerable<ICommandData> items, JsonOperationContext context)
        {
            var commands = items as List<ICommandData>;

            Debug.Assert(commands != null);

            if (commands.Count == 0)
                return;

            BatchOptions options = null;
            if (Configuration.LoadRequestTimeoutInSec != null)
            {
                options = new BatchOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(Configuration.LoadRequestTimeoutInSec.Value)
                };
            }

            var batchCommand = new BatchCommand(DocumentConventions.Default, context, commands, options);
            
            try
            {
                AsyncHelpers.RunSync(() => _requestExecutor.ExecuteAsync(batchCommand, context, token: CancellationToken));
                _recentUrl = _requestExecutor.Url;
            }
            catch (OperationCanceledException e)
            {
                if (CancellationToken.IsCancellationRequested == false)
                {
                    ThrowTimeoutException(commands.Count, e);
                }

                throw;
            }
        }

        protected override bool ShouldFilterOutHiLoDocument()
        {
            // if we transfer all documents to the same collections (no script specified) then don't exclude HiLo docs
            return string.IsNullOrEmpty(Transformation.Script) == false;
        }

        private static void ThrowTimeoutException(int numberOfCommands, Exception e)
        {
            var message = $"Load request applying {numberOfCommands} commands timed out.";

            throw new TimeoutException(message, e);
        }

        public override void Dispose()
        {
            base.Dispose();

            _serverStore.Server.ServerCertificateChanged -= OnServerCertificateChanged;

            _requestExecutor?.Dispose();
        }
    }
}
