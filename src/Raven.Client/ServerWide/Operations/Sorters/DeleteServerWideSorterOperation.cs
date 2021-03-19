using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Sorters
{
    public class DeleteServerWideSorterOperation : IServerOperation
    {
        private readonly string _sorterName;

        public DeleteServerWideSorterOperation(string sorterName)
        {
            _sorterName = sorterName ?? throw new ArgumentNullException(nameof(sorterName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteServerWideSorterCommand(_sorterName);
        }

        private class DeleteServerWideSorterCommand : RavenCommand, IRaftCommand
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
