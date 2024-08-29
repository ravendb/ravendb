using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Sorters
{
    /// <summary>
    /// Deletes a custom sorter from the RavenDB server using the DeleteSorterOperation.
    /// Once removed, the sorter will no longer be available for ordering query results in the associated database.
    /// </summary>
    public sealed class DeleteSorterOperation : IMaintenanceOperation
    {
        private readonly string _sorterName;

        /// <inheritdoc cref="DeleteSorterOperation" />
        /// <param name="sorterName">The name of the custom sorter to be deleted from the server.</param>
        public DeleteSorterOperation(string sorterName)
        {
            _sorterName = sorterName ?? throw new ArgumentNullException(nameof(sorterName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteSorterCommand(_sorterName);
        }

        private sealed class DeleteSorterCommand : RavenCommand, IRaftCommand
        {
            private readonly string _sorterName;

            public DeleteSorterCommand(string sorterName)
            {
                _sorterName = sorterName ?? throw new ArgumentNullException(nameof(sorterName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/sorters?name={Uri.EscapeDataString(_sorterName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
