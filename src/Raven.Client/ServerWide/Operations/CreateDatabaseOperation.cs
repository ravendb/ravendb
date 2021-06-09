using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class CreateDatabaseOperation : IServerOperation<DatabasePutResult>
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly int _replicationFactor;

        public CreateDatabaseOperation(DatabaseRecord databaseRecord)
        {
            if (databaseRecord == null)
                throw new ArgumentNullException(nameof(databaseRecord));

            ResourceNameValidator.AssertValidDatabaseName(databaseRecord.DatabaseName);
            _databaseRecord = databaseRecord;
            _replicationFactor = databaseRecord.Topology?.ReplicationFactor > 0 ? databaseRecord.Topology.ReplicationFactor : 1;
        }

        public CreateDatabaseOperation(DatabaseRecord databaseRecord, int replicationFactor)
        {
            if (databaseRecord == null)
                throw new ArgumentNullException(nameof(databaseRecord));

            if (replicationFactor <= 0)
                throw new ArgumentException(nameof(replicationFactor));

            ResourceNameValidator.AssertValidDatabaseName(databaseRecord.DatabaseName);
            _databaseRecord = databaseRecord;
            _replicationFactor = replicationFactor;
        }

        public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new CreateDatabaseCommand(_databaseRecord, _replicationFactor);
        }

        internal class CreateDatabaseCommand : RavenCommand<DatabasePutResult>, IRaftCommand
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
                var databaseDocument = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_databaseRecord, ctx);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, databaseDocument).ConfigureAwait(false))
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
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
