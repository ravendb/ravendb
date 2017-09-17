using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    class ReorderOperation : IServerOperation
    {
        public class Parameters
        {
            public List<string> MembersOrder;
        }

        private readonly string _database;
        private readonly Parameters _parameters;

        public ReorderOperation(string database, List<string> order)
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
            var order = EntityToBlittable.ConvertEntityToBlittable(_parameters, conventions, context);
            return new ReorderCommand(_database, order);
        }

        private class ReorderCommand : RavenCommand
        {
            private readonly string _databaseName;
            private readonly BlittableJsonReaderObject _orderBlittable;

            public ReorderCommand(string databaseName, BlittableJsonReaderObject orderBlittable)
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
