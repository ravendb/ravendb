using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Sparrow.Json;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Documents.Handlers
{
    public class LegacyReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/replication/lastEtag", "GET", AuthorizationStatus.ValidUser)]
        public Task LastEtag()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var sourceReplicationDocument = GetSourceReplicationInformation(context, GetRemoteServerInstanceId(), out _);
                var blittable = EntityToBlittable.ConvertCommandToBlittable(sourceReplicationDocument, context);
                context.Write(writer, blittable);
                writer.Flush();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/replicateDocs", "POST", AuthorizationStatus.ValidUser)]
        public async Task Documents()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var stream = new ArrayStream(RequestBodyStream(), "Docs"))
            using (var source = new StreamSource(stream, context, Database))
            {
                var destination = new DatabaseDestination(Database);
                var options = new DatabaseSmugglerOptionsServerSide
                {
                    ReadLegacyEtag = true,
                    OperateOnTypes = DatabaseItemType.Documents
                };

                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options);
                var result = smuggler.Execute();

                var replicationSource = GetSourceReplicationInformation(context, GetRemoteServerInstanceId(), out var documentId);
                replicationSource.LastDocumentEtag = result.LegacyLastDocumentEtag;
                replicationSource.Source = GetFromServer();
                replicationSource.LastBatchSize = result.Documents.ReadCount + result.Tombstones.ReadCount;
                replicationSource.LastModified = DateTime.UtcNow;

                await SaveSourceReplicationInformation(replicationSource, context, documentId);
            }
        }

        [RavenAction("/databases/*/replication/heartbeat", "POST", AuthorizationStatus.ValidUser)]
        public Task Heartbeat()
        {
            // nothing to do here
            return Task.CompletedTask;
        }
        
        [RavenAction("/databases/*/indexes/last-queried", "POST", AuthorizationStatus.ValidUser)]
        public Task LastQueried()
        {
            // nothing to do here
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/side-by-side-indexes", "PUT", AuthorizationStatus.ValidUser)]
        public Task SideBySideIndexes()
        {
            // nothing to do here
            return Task.CompletedTask;
        }

        private Guid GetRemoteServerInstanceId()
        {
            var remoteServerIdString = GetQueryStringValueAndAssertIfSingleAndNotEmpty("dbid");
            return Guid.Parse(remoteServerIdString);
        }

        private string GetFromServer()
        {
            var fromServer = GetQueryStringValueAndAssertIfSingleAndNotEmpty("from");

            if (string.IsNullOrEmpty(fromServer))
                throw new ArgumentException($"from cannot be null or empty", "from");

            while (fromServer.EndsWith("/"))
                fromServer = fromServer.Substring(0, fromServer.Length - 1); // remove last /, because that has special meaning for Raven

            return fromServer;
        }

        private LegacySourceReplicationInformation GetSourceReplicationInformation(DocumentsOperationContext context, Guid remoteServerInstanceId, out string documentId)
        {
            documentId = $"Raven/Replication/Sources/{remoteServerInstanceId}";

            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, documentId);
                if (document == null)
                {
                    return new LegacySourceReplicationInformation
                    {
                        ServerInstanceId = Database.DbId
                    };
                }

                return JsonDeserializationServer.LegacySourceReplicationInformation(document.Data);
            }
        }

        private async Task SaveSourceReplicationInformation(LegacySourceReplicationInformation replicationSource, DocumentsOperationContext context, string documentId)
        {
            var blittable = EntityToBlittable.ConvertCommandToBlittable(replicationSource, context);
            using (var cmd = new MergedPutCommand(blittable, documentId, null, Database))
            {
                await Database.TxMerger.Enqueue(cmd);
            }
        }
    }

    public class LegacySourceReplicationInformation
    {
        public LegacySourceReplicationInformation()
        {
            LastDocumentEtag = LastEtagsInfo.EtagEmpty;
            LastAttachmentEtag = LastEtagsInfo.EtagEmpty;
        }

        public string LastDocumentEtag { get; set; }

        public string LastAttachmentEtag { get; set; }

        public Guid ServerInstanceId { get; set; }

        public string Source { get; set; }

        public DateTime? LastModified { get; set; }

        public long LastBatchSize { get; set; }
    }
}
