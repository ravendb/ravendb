using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ConfigureRevisionsForConflictsOperation : IServerOperation<ConfigureRevisionsForConflictsResult>
    {
        private readonly string _database;
        private readonly RevisionsCollectionConfiguration _configuration;

        public ConfigureRevisionsForConflictsOperation(string database, RevisionsCollectionConfiguration configuration)
        {
            ResourceNameValidator.AssertValidDatabaseName(database);
            _database = database;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<ConfigureRevisionsForConflictsResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureRevisionsForConflictsCommand(conventions, _database, _configuration);
        }

        private class ConfigureRevisionsForConflictsCommand : RavenCommand<ConfigureRevisionsForConflictsResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly RevisionsCollectionConfiguration _configuration;

            public ConfigureRevisionsForConflictsCommand(
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
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx);
                        await ctx.WriteAsync(stream, config).ConfigureAwait(false);
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
