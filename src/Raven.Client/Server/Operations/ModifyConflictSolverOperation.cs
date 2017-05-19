using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ModifyConflictSolverOperation : IServerOperation<ModifySolverResult>
    {
        private readonly string _database;
        public string ResolverDbId;
        public Dictionary<string, ScriptResolver> CollectionByScript;
        public bool ResolveToLatest;

        public ModifyConflictSolverOperation(string database,string resolverDbId = null, Dictionary<string,ScriptResolver> collectionByScript = null, bool resolveToLatest = false)
        {
            MultiDatabase.AssertValidName(database);
            _database = database;
            ResolverDbId = resolverDbId;
            CollectionByScript = collectionByScript;
            ResolveToLatest = resolveToLatest;
        }

        public RavenCommand<ModifySolverResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ModifyConflictSolverCommand(conventions, context, _database, this);
        }

        private class ModifyConflictSolverCommand : RavenCommand<ModifySolverResult>
        {
            private readonly ModifyConflictSolverOperation _solver;
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
    
            public ModifyConflictSolverCommand(
                DocumentConventions conventions,
                JsonOperationContext context,
                string database,
                ModifyConflictSolverOperation solver)
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _solver = solver;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/update-resolver?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var solver = EntityToBlittable.ConvertEntityToBlittable(new ConflictSolver
                        {
                            ResolveByCollection = _solver.CollectionByScript,
                            ResolveToLatest = _solver.ResolveToLatest,
                            DatabaseResolverId = _solver.ResolverDbId
                        }, _conventions, _context);
                        _context.Write(stream, solver);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifySolverResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
