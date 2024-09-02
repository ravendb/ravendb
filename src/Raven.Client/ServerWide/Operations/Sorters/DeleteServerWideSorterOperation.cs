using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Sorters
{
    /// <summary>
    /// Server-wide operation to delete custom sorter definition from the server.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Operations.ServerOperations.Sorters.CustomSorters"/>
    public sealed class DeleteServerWideSorterOperation : IServerOperation
    {
        private readonly string _sorterName;

        /// <inheritdoc cref="DeleteServerWideSorterOperation"/>
        /// <param name="sorterName">Name of custom sorter to delete.</param>
        /// <exception cref="ArgumentNullException">Thrown when `sorterName` is null or empty.</exception>
        public DeleteServerWideSorterOperation(string sorterName)
        {
            _sorterName = sorterName ?? throw new ArgumentNullException(nameof(sorterName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteServerWideSorterCommand(_sorterName);
        }

        private sealed class DeleteServerWideSorterCommand : RavenCommand, IRaftCommand
        {
            private readonly string _sorterName;

            public DeleteServerWideSorterCommand(string sorterName)
            {
                _sorterName = sorterName ?? throw new ArgumentNullException(nameof(sorterName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/sorters?name={Uri.EscapeDataString(_sorterName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
