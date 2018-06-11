using System.Net.Http;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class StartTransactionsRecordingCommand : RavenCommand
    {
        private readonly string _filePath;

        public StartTransactionsRecordingCommand(string filePath)
        {
            _filePath = filePath;
        }
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/start_transactions_recording?file_path={_filePath}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }
    }

    public class StopTransactionsRecordingCommand : RavenCommand
    {
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/stop_transactions_recording";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }
    }

    public class ReplayTransactionsRecordingCommand : RavenCommand
    {
        private readonly string _filePath;

        public ReplayTransactionsRecordingCommand(string filePath)
        {
            _filePath = filePath;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replay_transactions";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    var jsonReaderObject = EntityToBlittable.ConvertEntityToBlittable(
                            new { file_path = _filePath },
                            DocumentConventions.Default,
                            ctx, new JsonSerializer(),
                            null
                        )
                        ;
                    ctx.Write(stream, jsonReaderObject);
                })
            };
            return request;
        }
    }
}
