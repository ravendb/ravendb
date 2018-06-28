using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    public class ReplayTransactionsRecordingOperation : IMaintenanceOperation
    {
        private readonly Stream _replayStream;

        public ReplayTransactionsRecordingOperation(Stream replayStream)
        {
            _replayStream = replayStream;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ReplayTransactionsRecordingCommand(_replayStream);
        }

        private class ReplayTransactionsRecordingCommand : RavenCommand
        {
            private readonly Stream _replayStream;

            public ReplayTransactionsRecordingCommand(Stream replayStream)
            {
                _replayStream = replayStream;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transactions/replay?";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new StreamContent(_replayStream)
                };
                return request;
            }
        }
    }
}
