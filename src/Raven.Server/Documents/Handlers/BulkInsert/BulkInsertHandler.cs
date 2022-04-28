using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.BulkInsert;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.BulkInsert
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_insert", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkInsert()
        {
            var operationCancelToken = CreateOperationToken();
            var id = GetLongQueryString("id");
            var skipOverwriteIfUnchanged = GetBoolValueQueryString("skipOverwriteIfUnchanged", required: false) ?? false;

            await Database.Operations.AddOperation(Database, "Bulk Insert", Operations.Operations.OperationType.BulkInsert,
                async progress =>
                {
                    using (var bulkInsertProcessor = new BulkInsertHandlerProcessor(this, Database, progress, skipOverwriteIfUnchanged, operationCancelToken.Token))
                    {
                        await bulkInsertProcessor.ExecuteAsync();

                        return bulkInsertProcessor.OperationResult;
                    }
                },
                id,
                token: operationCancelToken
            );
        }
    }
}
