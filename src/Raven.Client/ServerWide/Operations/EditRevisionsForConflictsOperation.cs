using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class EditRevisionsForConflictsOperation : IServerOperation<EditRevisionsForConflictsOperationResult>
    {
        private readonly string _database;
        private readonly RevisionsCollectionConfiguration _configuration;

        public EditRevisionsForConflictsOperation(string database, RevisionsCollectionConfiguration configuration)
        {
            Helpers.AssertValidDatabaseName(database);
            _database = database;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<EditRevisionsForConflictsOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigRevisionsOnConflictCommand(conventions, _database, _configuration);
        }

        private class ConfigRevisionsOnConflictCommand : RavenCommand<EditRevisionsForConflictsOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly RevisionsCollectionConfiguration _configuration;

            public ConfigRevisionsOnConflictCommand(
                DocumentConventions conventions,
                string database,
                RevisionsCollectionConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _configuration = configuration;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/revisions/conflicts/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertCommandToBlittable(_configuration, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigRevisionsOnConflictOperationResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
