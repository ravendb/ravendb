using System;
using System.Net.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class CreateDatabaseOperation : IServerOperation<CreateDatabaseResult>
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly int _replicationFactor;

        public CreateDatabaseOperation(DatabaseRecord databaseRecord, int replicationFactor = 1)
        {
            MultiDatabase.AssertValidName(databaseRecord.DatabaseName);
            _databaseRecord = databaseRecord;
            _replicationFactor = replicationFactor;
        }

        public RavenCommand<CreateDatabaseResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateDatabaseCommand(conventions, context, _databaseRecord, this);
        }

        private class CreateDatabaseCommand : RavenCommand<CreateDatabaseResult>
        {
            private readonly JsonOperationContext _context;
            private readonly CreateDatabaseOperation _createDatabaseOperation;
            private readonly BlittableJsonReaderObject _databaseDocument;
            private readonly string _databaseName;

            public CreateDatabaseCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseRecord databaseRecord,
                CreateDatabaseOperation createDatabaseOperation)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (databaseRecord == null)
                    throw new ArgumentNullException(nameof(databaseRecord));

                _context = context;
                _createDatabaseOperation = createDatabaseOperation;
                _databaseName = databaseRecord.DatabaseName;
                _databaseDocument = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, conventions, context);
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={_databaseName}";
                
                url += "&replication-factor=" + _createDatabaseOperation._replicationFactor;

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _databaseDocument);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.CreateDatabaseResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}