using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Analyzers
{
    public class DeleteServerWideAnalyzerOperation : IServerOperation
    {
        private readonly string _analyzerName;

        public DeleteServerWideAnalyzerOperation(string analyzerName)
        {
            _analyzerName = analyzerName ?? throw new ArgumentNullException(nameof(analyzerName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteServerWideAnalyzerCommand(_analyzerName);
        }

        private class DeleteServerWideAnalyzerCommand : RavenCommand, IRaftCommand
        {
            private readonly string _analyzerName;

            public DeleteServerWideAnalyzerCommand(string analyzerName)
            {
                _analyzerName = analyzerName ?? throw new ArgumentNullException(nameof(analyzerName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/analyzers?name={Uri.EscapeDataString(_analyzerName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
