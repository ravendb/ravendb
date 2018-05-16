using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class CreateDatabaseOperation : IServerOperation<DatabasePutResult>
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly int _replicationFactor;

        public CreateDatabaseOperation(DatabaseRecord databaseRecord, int replicationFactor = 1)
        {
            Helpers.AssertValidDatabaseName(databaseRecord.DatabaseName);
            _databaseRecord = databaseRecord;
            _replicationFactor = replicationFactor;
        }

        public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new CreateDatabaseCommand(_databaseRecord, _replicationFactor);
        }

        internal class CreateDatabaseCommand : RavenCommand<DatabasePutResult>
        {
            private readonly DatabaseRecord _databaseRecord;
            private readonly int _replicationFactor;
            private readonly long? _etag;
            private readonly string _databaseName;

            public CreateDatabaseCommand(DatabaseRecord databaseRecord, int replicationFactor = 1, long? etag = null)
            {
                _databaseRecord = databaseRecord;
                _replicationFactor = replicationFactor;
                _etag = etag;
                _databaseName = databaseRecord?.DatabaseName ?? throw new ArgumentNullException(nameof(databaseRecord));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={_databaseName}";

                url += "&replicationFactor=" + _replicationFactor;
                var databaseDocument = EntityToBlittable.ConvertCommandToBlittable(_databaseRecord, ctx);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, databaseDocument);
                    })
                };

                if (_etag.HasValue)
                    request.Headers.TryAddWithoutValidation(Constants.Headers.Etag, $"\"{_etag.ToInvariantString()}\"");

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.DatabasePutResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
