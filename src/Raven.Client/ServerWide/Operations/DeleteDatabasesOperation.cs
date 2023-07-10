using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class DeleteDatabasesOperation : IServerOperation<DeleteDatabaseResult>
    {
        private readonly Parameters _parameters;
        
        public DeleteDatabasesOperation(string databaseName, bool hardDelete) : this(databaseName, hardDelete, fromNode: null, timeToWaitForConfirmation: TimeSpan.FromSeconds(15))
        {
            
        }

        public DeleteDatabasesOperation(string databaseName, bool hardDelete, string fromNode) : this(databaseName, hardDelete, fromNode: fromNode, timeToWaitForConfirmation: TimeSpan.FromSeconds(15))
        {
           
        }

        public DeleteDatabasesOperation(string databaseName, int shardNumber, bool hardDelete, string fromNode, TimeSpan? timeToWaitForConfirmation = null) : 
            this(ClientShardHelper.ToShardName(databaseName, shardNumber), hardDelete, fromNode: fromNode, timeToWaitForConfirmation: timeToWaitForConfirmation)
        {
            
        }
        
        public DeleteDatabasesOperation(string databaseName, bool hardDelete, string fromNode = null, TimeSpan? timeToWaitForConfirmation = null)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            _parameters = new Parameters
            {
                DatabaseNames = new[] { databaseName },
                HardDelete = hardDelete,
                TimeToWaitForConfirmation = timeToWaitForConfirmation
            };

            if (fromNode != null)
                _parameters.FromNodes = new[] { fromNode };
        }

        public DeleteDatabasesOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.DatabaseNames == null || parameters.DatabaseNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.DatabaseNames));

            foreach (var databaseName in parameters.DatabaseNames)
            {
                if (databaseName == null)
                    throw new ArgumentNullException(nameof(databaseName));

                var dbName = databaseName;
                if (ClientShardHelper.IsShardName(databaseName))
                {
                    ClientShardHelper.TryGetShardNumberAndDatabaseName(databaseName, out dbName, out _);

                    if (parameters.FromNodes == null)
                        throw new ArgumentException($"Must specify node when deleting a shard.");
                }

                ResourceNameValidator.AssertValidDatabaseName(dbName);
            }

            _parameters = parameters;
        }

        public RavenCommand<DeleteDatabaseResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteDatabaseCommand(conventions, context, _parameters);
        }

        private class DeleteDatabaseCommand : RavenCommand<DeleteDatabaseResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _parameters;

            public DeleteDatabaseCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));
                _conventions = conventions;

                _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.DeleteDatabaseResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        public class Parameters
        {
            public string[] DatabaseNames { get; set; }

            public bool HardDelete { get; set; }

            public string[] FromNodes { get; set; }

            public TimeSpan? TimeToWaitForConfirmation { get; set; }
        }
    }
}
