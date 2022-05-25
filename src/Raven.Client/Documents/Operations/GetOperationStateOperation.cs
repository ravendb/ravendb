using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Client.Documents.Operations
{
    public class GetOperationStateOperation : IMaintenanceOperation<OperationState>
    {
        private readonly long _id;
        private readonly string _nodeTag;

        public GetOperationStateOperation(long id)
        {
            _id = id;
        }

        public GetOperationStateOperation(long id, string nodeTag)
        {
            _id = id;
            _nodeTag = nodeTag;
        }

        public RavenCommand<OperationState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetOperationStateCommand(_id, _nodeTag);
        }

        internal class GetOperationStateCommand : RavenCommand<OperationState>
        {
            public override bool IsReadRequest => true;

            private readonly long _id;

            public GetOperationStateCommand(long id, string nodeTag = null)
            {
                _id = id;
                SelectedNodeTag = nodeTag;
                Timeout = TimeSpan.FromSeconds(15);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/operations/state?id={_id}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<OperationState>(response);
            }

            internal static void CombineSmugglerResults(IOperationResult finalResult, IOperationResult result)
            {
                switch (result)
                {
                    case BackupResult backupResult:
                        CombineBackupResults((BackupResult)finalResult, backupResult);
                        break;
                    case SmugglerResult smugglerResult:
                        CombineSmugglerResults((SmugglerResult)finalResult, smugglerResult);
                        break;
                    default:
                        throw new ArgumentException($"Not supported type {result.GetType()}");
                }
            }

            private static void CombineSmugglerResults(SmugglerResult finalResult, SmugglerResult smugglerResult)
            {
                finalResult.Documents.SkippedCount += smugglerResult.Documents.SkippedCount;
                finalResult.Documents.ReadCount += smugglerResult.Documents.ReadCount;
                finalResult.Documents.ErroredCount += smugglerResult.Documents.ErroredCount;
                finalResult.Documents.LastEtag = Math.Max(finalResult.Documents.LastEtag, smugglerResult.Documents.LastEtag);
                finalResult.Documents.Attachments.ReadCount += smugglerResult.Documents.Attachments.ReadCount;

                finalResult.Tombstones.ReadCount += smugglerResult.Tombstones.ReadCount;
                finalResult.Tombstones.ErroredCount += smugglerResult.Tombstones.ErroredCount;
                finalResult.Tombstones.LastEtag = Math.Max(finalResult.Tombstones.LastEtag, smugglerResult.Tombstones.LastEtag);

                finalResult.RevisionDocuments.ReadCount += smugglerResult.RevisionDocuments.ReadCount;
                finalResult.RevisionDocuments.ErroredCount += smugglerResult.RevisionDocuments.ErroredCount;
                finalResult.RevisionDocuments.LastEtag = Math.Max(finalResult.RevisionDocuments.LastEtag, smugglerResult.RevisionDocuments.LastEtag);
                finalResult.RevisionDocuments.Attachments = smugglerResult.RevisionDocuments.Attachments;

                finalResult.Counters.ReadCount += smugglerResult.Counters.ReadCount;
                finalResult.Counters.ErroredCount += smugglerResult.Counters.ErroredCount;
                finalResult.Counters.LastEtag = Math.Max(finalResult.Counters.LastEtag, smugglerResult.Counters.LastEtag);

                finalResult.TimeSeries.ReadCount += smugglerResult.TimeSeries.ReadCount;
                finalResult.TimeSeries.ErroredCount += smugglerResult.TimeSeries.ErroredCount;
                finalResult.TimeSeries.LastEtag = Math.Max(finalResult.TimeSeries.LastEtag, smugglerResult.TimeSeries.LastEtag);

                finalResult.Identities.ReadCount += smugglerResult.Identities.ReadCount;
                finalResult.Identities.ErroredCount += smugglerResult.Identities.ErroredCount;

                finalResult.CompareExchange.ReadCount += smugglerResult.CompareExchange.ReadCount;
                finalResult.CompareExchange.ErroredCount += smugglerResult.CompareExchange.ErroredCount;

                finalResult.Subscriptions.ReadCount += smugglerResult.Subscriptions.ReadCount;
                finalResult.Subscriptions.ErroredCount += smugglerResult.Subscriptions.ErroredCount;

                finalResult.Indexes.ReadCount += smugglerResult.Indexes.ReadCount;
                finalResult.Indexes.ErroredCount += smugglerResult.Indexes.ErroredCount;

                foreach (var message in smugglerResult.Messages)
                    finalResult.AddMessage(message);
            }

            internal static void CombineBackupResults(BackupResult finalResult, BackupResult backupResult)
            {
                CombineSmugglerResults(finalResult, backupResult);

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal,
                    "need to combine all other BackupResult properties, e.g. LocalBackup, S3Backup, etc.");
            }
        }
    }
}
