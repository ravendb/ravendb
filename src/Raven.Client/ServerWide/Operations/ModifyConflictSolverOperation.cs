using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ModifyConflictSolverOperation : IServerOperation<ModifySolverResult>
    {
        private readonly string _database;
        public Dictionary<string, ScriptResolver> CollectionByScript;
        public bool ResolveToLatest;

        public ModifyConflictSolverOperation(string database, Dictionary<string, ScriptResolver> collectionByScript = null, bool resolveToLatest = false)
        {
            ResourceNameValidator.AssertValidDatabaseName(database);
            _database = database;
            CollectionByScript = collectionByScript;
            ResolveToLatest = resolveToLatest;
        }

        public RavenCommand<ModifySolverResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ModifyConflictSolverCommand(conventions, _database, this);
        }

        private class ModifyConflictSolverCommand : RavenCommand<ModifySolverResult>, IRaftCommand
        {
            private readonly ModifyConflictSolverOperation _solver;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;

            public ModifyConflictSolverCommand(
                DocumentConventions conventions,
                string database,
                ModifyConflictSolverOperation solver)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _solver = solver;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/replication/conflicts/solver?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var solver = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new ConflictSolver
                        {
                            ResolveByCollection = _solver.CollectionByScript,
                            ResolveToLatest = _solver.ResolveToLatest,
                        }, ctx);
                        await ctx.WriteAsync(stream, solver).ConfigureAwait(false);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifySolverResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
