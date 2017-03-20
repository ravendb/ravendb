using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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

        public RavenEtl(DocumentDatabase database, RavenEtlConfiguration configuration) : base(database, configuration, RavenEtlTag)
        {
            EtlConfiguration = configuration;
            _requestExecutor = RequestExecutor.CreateForSingleNode(EtlConfiguration.Url, EtlConfiguration.Database, EtlConfiguration.ApiKey);
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
            return new RavenEtlDocumentTransformer(Database, context, EtlConfiguration);
        }

        protected override void LoadInternal(IEnumerable<ICommandData> items, JsonOperationContext context)
        {
            var commands = items as List<ICommandData>;

            Debug.Assert(commands != null);

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
                    throw new TimeoutException($"Load request applying the following {commands.Count} commands timed out: " +
                                               $"{string.Join(", ", commands.Select(x => $"{x.Key} ({x.Method})"))}", e);
                }

                throw;
            }
        }

        public override bool CanContinueBatch()
        {
            return true; // TODO 
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