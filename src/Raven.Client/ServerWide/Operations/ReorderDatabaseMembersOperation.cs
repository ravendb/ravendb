using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    /// <summary>
    /// Allows to change the order of nodes in the database group topology.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Operations.ServerOperations.ReorderDatabaseMembersOperation"/>
    public sealed class ReorderDatabaseMembersOperation : IServerOperation
    {
        public sealed class Parameters
        {
            public List<string> MembersOrder;
            public bool Fixed;
        }

        private readonly string _database;
        private readonly Parameters _parameters;

        /// <inheritdoc cref="ReorderDatabaseMembersCommand"/>
        /// <param name="database">Name of a database to operate on.</param>
        /// <param name="order">List of node tags in the exact desired order.</param>
        public ReorderDatabaseMembersOperation(string database, List<string> order) : this(database, order, false)
        {
        }

        /// <inheritdoc cref="ReorderDatabaseMembersCommand"/>
        /// <param name="database">Name of a database to operate on.</param>
        /// <param name="order">List of node tags in the exact desired order.</param>
        /// <param name="fixedTopology">When set to true, the cluster will try to remain provided nodes order. Otherwise, it may be changed after being initially set.</param>
        /// <exception cref="ArgumentException">Thrown when the reordered list doesn't correspond to the existing nodes of the database group.</exception>
        public ReorderDatabaseMembersOperation(string database, List<string> order, bool fixedTopology)
        {
            if (order == null || order.Count == 0)
                throw new ArgumentException("Order list must contain values");

            _database = database;
            _parameters = new Parameters
            {
                MembersOrder = order,
                Fixed = fixedTopology
            };
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            var order = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, context);
            return new ReorderDatabaseMembersCommand(conventions, _database, order);
        }

        private sealed class ReorderDatabaseMembersCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly BlittableJsonReaderObject _orderBlittable;

            public ReorderDatabaseMembersCommand(DocumentConventions conventions, string databaseName, BlittableJsonReaderObject orderBlittable)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);

                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = databaseName;
                _orderBlittable = orderBlittable;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/reorder?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _orderBlittable).ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
