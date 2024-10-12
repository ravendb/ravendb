using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.BulkInsert;
using Raven.Server.Documents.Operations;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.BulkInsert
{
    public sealed class BulkInsertHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_insert", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkInsert()
        {
            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var id = GetLongQueryString("id");
            var skipOverwriteIfUnchanged = GetBoolValueQueryString("skipOverwriteIfUnchanged", required: false) ?? false;

            await Database.Operations.AddLocalOperation(
                id,
                OperationType.BulkInsert,
                 "Bulk Insert",
                detailedDescription: null,
                async progress =>
                {
                    using (var bulkInsertProcessor = new BulkInsertHandlerProcessor(this, Database, progress, skipOverwriteIfUnchanged, operationCancelToken.Token))
                    {
                        if (Database.ForTestingPurposes != null)
                        {
                            if (Database.ForTestingPurposes.BulkInsert_StreamReadTimeout > 0)
                                bulkInsertProcessor.ForTestingPurposesOnly().BulkInsert_StreamReadTimeout = Database.ForTestingPurposes.BulkInsert_StreamReadTimeout;

                            if (Database.ForTestingPurposes.BulkInsert_OnHeartBeat != null)
                                bulkInsertProcessor.ForTestingPurposesOnly().BulkInsert_OnHeartBeat = Database.ForTestingPurposes.BulkInsert_OnHeartBeat;
                        }

                        await bulkInsertProcessor.ExecuteAsync();

                        return bulkInsertProcessor.OperationResult;
                    }
                },
                token: operationCancelToken
            );
        }
    }
}
