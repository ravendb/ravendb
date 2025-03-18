using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    /// <summary>
    /// Operation to replay a recorded transaction.
    /// This allows re-executing previously recorded transactions for debugging or recovery purposes.
    /// </summary>
    public sealed class ReplayTransactionsRecordingOperation : IMaintenanceOperation<ReplayTxOperationResult>
    {
        private readonly Stream _replayStream;
        private readonly long _operationId;

        /// <inheritdoc cref="ReplayTransactionsRecordingOperation"/>
        /// <param name="replayStream">The stream containing the recorded transaction.</param>
        /// <param name="operationId">A unique identifier for the replay operation.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="replayStream"/> does not have its position set to zero.
        /// </exception>
        public ReplayTransactionsRecordingOperation(Stream replayStream, long operationId)
        {
            _replayStream = replayStream;
            _operationId = operationId;
            if (_replayStream.Position != 0)
                throw new ArgumentException("For replay transactions recording the stream position must to be set to zero");
        }

        public RavenCommand<ReplayTxOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ReplayTransactionsRecordingCommand(_replayStream, _operationId);
        }

        private sealed class ReplayTransactionsRecordingCommand : RavenCommand<ReplayTxOperationResult>
        {
            private readonly Stream _replayStream;
            private readonly long _operationId;

            public ReplayTransactionsRecordingCommand(Stream replayStream, long operationId)
            {
                _replayStream = replayStream;
                _operationId = operationId;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transactions/replay?operationId={_operationId}";

                var form = new MultipartFormDataContent
                {
                    {new StreamContent(_replayStream), "file", "name"}
                };
                
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = form
                };
                _replayStream.Position = 0;
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetReplayTrxOperationResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
