using System.Net.Http;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    public class StartTransactionsRecordingOperation : IMaintenanceOperation
    {
        private readonly string _filePath;

        public StartTransactionsRecordingOperation(string filePath)
        {
            _filePath = filePath;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartTransactionsRecordingCommand(_filePath);
        }

        private class StartTransactionsRecordingCommand : RavenCommand
        {
            private readonly string _filePath;

            public StartTransactionsRecordingCommand(string filePath)
            {
                _filePath = filePath;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transactions/start-recording?file={_filePath}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var jsonReaderObject = EntityToBlittable.ConvertEntityToBlittable(
                                entity: new { file = _filePath },
                                DocumentConventions.Default,
                                ctx,
                                new JsonSerializer(),
                                documentInfo: null
                            );
                        ctx.Write(stream, jsonReaderObject);
                    })
                };
                return request;
            }
        }
    }
}
