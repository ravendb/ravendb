using System;
using System.Net.Http;
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
        private readonly DatabaseDocument _databaseDocument;
        private readonly int _replicationFactor;

        public CreateDatabaseOperation(DatabaseDocument databaseDocument, int replicationFactor = 1)
        {
            MultiDatabase.AssertValidName(databaseDocument.Id);
            _databaseDocument = databaseDocument;
            _replicationFactor = replicationFactor;
        }

        public RavenCommand<CreateDatabaseResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateDatabaseCommand(conventions, context, _databaseDocument, this);
        }

        private class CreateDatabaseCommand : RavenCommand<CreateDatabaseResult>
        {
            private readonly JsonOperationContext _context;
            private readonly CreateDatabaseOperation _createDatabaseOperation;
            private readonly BlittableJsonReaderObject _databaseDocument;
            private readonly string _databaseName;

            public CreateDatabaseCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseDocument databaseDocument,
                CreateDatabaseOperation createDatabaseOperation)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (databaseDocument == null)
                    throw new ArgumentNullException(nameof(databaseDocument));

                _context = context;
                _createDatabaseOperation = createDatabaseOperation;
                _databaseName = databaseDocument.Id;
                _databaseDocument = EntityToBlittable.ConvertEntityToBlittable(databaseDocument, conventions, context);
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