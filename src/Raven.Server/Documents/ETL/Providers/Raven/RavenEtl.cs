using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Providers.Raven.Enumerators;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtl : EtlProcess<RavenEtlItem, ICommandData>
    {
        public const string RavenEtlTag = "Raven ETL";

        private readonly RequestExecutor _requestExecutor;

        private readonly RavenEtlDocumentTransformer.ScriptInput _script;

        public RavenEtl(DocumentDatabase database, RavenEtlConfiguration configuration) : base(database, configuration, RavenEtlTag)
        {
            EtlConfiguration = configuration;
            _requestExecutor = RequestExecutor.CreateForSingleNode(EtlConfiguration.Url, EtlConfiguration.Database, EtlConfiguration.ApiKey);
            _script = new RavenEtlDocumentTransformer.ScriptInput(configuration);
        }

        public RavenEtlConfiguration EtlConfiguration { get; }

        protected override IEnumerator<RavenEtlItem> ConvertDocsEnumerator(IEnumerator<Document> docs)
        {
            return new DocumentsToRavenEtlItems(docs);
        }

        protected override IEnumerator<RavenEtlItem> ConvertTombstonesEnumerator(IEnumerator<DocumentTombstone> tombstones)
        {
            return new TombstonesToRavenEtlItems(tombstones);
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
            if (EtlConfiguration.LoadRequestTimeoutInSec != null)
            {
                options = new BatchOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(EtlConfiguration.LoadRequestTimeoutInSec.Value)
                };
            }

            var batchCommand = new BatchCommand(new DocumentConventions(), context, commands, options);
            
            try
            {
                _requestExecutor.Execute(batchCommand, context, CancellationToken);
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

        private static void ThrowTimeoutException(int numberOfCommands, Exception e)
        {
            var message = $"Load request applying {numberOfCommands} commands timed out.";

            throw new TimeoutException(message, e);
        }

        protected override void UpdateMetrics(DateTime startTime, Stopwatch duration, int batchSize)
        {
            // TODO arek
        }

        public override void Dispose()
        {
            base.Dispose();
            _requestExecutor?.Dispose();
        }
    }
}