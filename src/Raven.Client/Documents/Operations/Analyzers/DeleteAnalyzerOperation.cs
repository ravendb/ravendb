using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Analyzers
{
    /// <summary>
    /// Deletes an analyzer from a specific database using the DeleteAnalyzerOperation.
    /// 
    /// <para><strong>Note:</strong> By default, this operation applies to the default database of the document store being used. 
    /// To target a different database, use the ForDatabase() method.</para>
    /// </summary>
    public sealed class DeleteAnalyzerOperation : IMaintenanceOperation
    {
        private readonly string _analyzerName;

        /// <inheritdoc cref="DeleteAnalyzerOperation" />
        /// <param name="analyzerName">The name of the analyzer to be deleted.</param>
        public DeleteAnalyzerOperation(string analyzerName)
        {
            _analyzerName = analyzerName ?? throw new ArgumentNullException(nameof(analyzerName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteAnalyzerCommand(_analyzerName);
        }

        private sealed class DeleteAnalyzerCommand : RavenCommand, IRaftCommand
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
