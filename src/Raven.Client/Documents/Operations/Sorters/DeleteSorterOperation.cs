using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Sorters
{
    public class DeleteSorterOperation : IMaintenanceOperation
    {
        private readonly string _sorterName;

        public DeleteSorterOperation(string sorterName)
        {
            _sorterName = sorterName ?? throw new ArgumentNullException(nameof(sorterName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteSorterCommand(_sorterName);
        }

        private class DeleteSorterCommand : RavenCommand, IRaftCommand
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
