using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Providers.Raven.Enumerators;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtl : EtlProcess<RavenEtlItem, ICommandData>
    {
        public const string RavenEtlTag = "Raven ETL";

        private readonly PatchRequest _transformationScript;

        public RavenEtl(DocumentDatabase database, RavenEtlConfiguration configuration) : base(database, configuration, RavenEtlTag)
        {
            EtlConfiguration = configuration;
            _transformationScript = new PatchRequest {Script = EtlConfiguration.Script};
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

        public override IEnumerable<ICommandData> Transform(IEnumerable<RavenEtlItem> items, DocumentsOperationContext context)
        {
            var transformer = new RavenEtlDocumentTransformer(Database);

            var commands = new List<ICommandData>();

            foreach (var item in items)
            {
                try
                {
                    if (item.IsDelete)
                        commands.Add(new DeleteCommandData(item.DocumentKey, null));
                    else
                    {
                        var result = transformer.Apply(context, item.Document, _transformationScript);

                        commands.Add(new PutCommandDataWithBlittableJson(item.DocumentKey, null, result.ModifiedDocument));
                    }

                    Statistics.TransformationSuccess();

                    CurrentBatch.LastTransformedEtag = item.Etag;

                    if (CanContinueBatch() == false)
                        break;
                }
                catch (JavaScriptParseException e)
                {
                    StopProcessOnScriptParseError(e);
                    break;
                }
                catch (Exception e)
                {
                    Statistics.RecordTransformationError(e);

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Could not process SQL ETL script for '{Name}', skipping document: {item.DocumentKey}", e);
                }
            }

            return commands;
        }

        protected override void LoadInternal(IEnumerable<ICommandData> commands, JsonOperationContext context)
        {
            using (var requestExecutor = RequestExecutor.ShortTermSingleUse(EtlConfiguration.Url, EtlConfiguration.Database, EtlConfiguration.ApiKey)) // TODO arek - consider caching it somewhere
            {
                var batchCommand = new BatchCommand(new DocumentConventions(), context, commands as List<ICommandData>);

                requestExecutor.Execute(batchCommand, context);
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
    }
}