using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Analyzers
{
    public class DeleteAnalyzerOperation : IMaintenanceOperation
    {
        private readonly string _analyzerName;

        public DeleteAnalyzerOperation(string analyzerName)
        {
            _analyzerName = analyzerName ?? throw new ArgumentNullException(nameof(analyzerName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteAnalyzerCommand(_analyzerName);
        }

        private class DeleteAnalyzerCommand : RavenCommand, IRaftCommand
        {
            private readonly string _analyzerName;

            public DeleteAnalyzerCommand(string analyzerName)
            {
                _analyzerName = analyzerName ?? throw new ArgumentNullException(nameof(analyzerName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/analyzers?name={Uri.EscapeDataString(_analyzerName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
