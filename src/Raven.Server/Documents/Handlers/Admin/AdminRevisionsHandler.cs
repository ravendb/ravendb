// -----------------------------------------------------------------------
//  <copyright file="AdminRevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminRevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/revisions", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteRevisionsFor()
        {
            bool includeForceCreated = GetBoolValueQueryString("includeForceCreated", required: false) ?? false;

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "admin/revisions/delete");
                var parameters = JsonDeserializationServer.Parameters.DeleteRevisionsParameters(json);

                using (var token = CreateTimeLimitedOperationToken())
                {
                    var ids = parameters.DocumentIds;
                    DeleteRevisionsCommand cmd;
                    do
                    {
                        token.Delay();
                        cmd = new DeleteRevisionsCommand(ids, Database, includeForceCreated, token);
                        await Database.TxMerger.Enqueue(cmd);
                    } while (cmd.MoreWork);
                }
                NoContentStatus();
            }
        }

        internal class DeleteRevisionsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Microsoft.Extensions.Primitives.StringValues _ids;
            private readonly DocumentDatabase _database;
            private readonly bool _includeForceCreated;
            private readonly OperationCancelToken _token;

            public bool MoreWork;

            public DeleteRevisionsCommand(string[] ids, DocumentDatabase database, bool includeForceCreated, OperationCancelToken token)
            {
                _ids = ids;
                _database = database;
                _includeForceCreated = includeForceCreated;
                _token = token;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var skipForceCreated = _includeForceCreated == false;
                MoreWork = false;
                foreach (var id in _ids)
                {
                    _token.ThrowIfCancellationRequested();
                    _database.DocumentsStorage.RevisionsStorage.DeleteAllRevisionsFor(context, id, skipForceCreated, ref MoreWork);
                }
                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
            {
                return new DeleteRevisionsCommandDto() { Ids = _ids, IncludeForceCreated = _includeForceCreated };
            }
        }

        public class Parameters
        {
            public string[] DocumentIds { get; set; }
        }
    }

    internal class DeleteRevisionsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<AdminRevisionsHandler.DeleteRevisionsCommand>
    {
        public string[] Ids;
        public bool IncludeForceCreated;

        public AdminRevisionsHandler.DeleteRevisionsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new AdminRevisionsHandler.DeleteRevisionsCommand(Ids, database, IncludeForceCreated, OperationCancelToken.None);
            return command;
        }
    }
}
