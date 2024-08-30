using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    /// <summary>
    /// Stops the recording of database transactions that was initiated with the StartTransactionsRecordingOperation.
    /// This operation finalizes the recording and closes the file where transactions were being saved.
    /// </summary>
    public sealed class StopTransactionsRecordingOperation : IMaintenanceOperation
    {
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StopTransactionsRecordingCommand();
        }

        private sealed class StopTransactionsRecordingCommand : RavenCommand
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/transactions/stop-recording";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
                return request;
            }
        }
    }
}
