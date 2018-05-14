using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ReorderDatabaseMembersOperation : IServerOperation
    {
        public class Parameters
        {
            public List<string> MembersOrder;
        }

        private readonly string _database;
        private readonly Parameters _parameters;

        public ReorderDatabaseMembersOperation(string database, List<string> order)
        {
            if (order == null || order.Count == 0)
                throw new ArgumentException("Order list must contain values");

            _database = database;
            _parameters = new Parameters
            {
                MembersOrder = order
            };
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            var order = EntityToBlittable.ConvertCommandToBlittable(_parameters, context);
            return new ReorderDatabaseMembersCommand(_database, order);
        }

        private class ReorderDatabaseMembersCommand : RavenCommand
        {
            private readonly string _databaseName;
            private readonly BlittableJsonReaderObject _orderBlittable;

            public ReorderDatabaseMembersCommand(string databaseName, BlittableJsonReaderObject orderBlittable)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);

                _databaseName = databaseName;
                _orderBlittable = orderBlittable;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/reorder?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _orderBlittable);
                    })
                };

                return request;
            }

            public override bool IsReadRequest => false;
        }

    }
}
