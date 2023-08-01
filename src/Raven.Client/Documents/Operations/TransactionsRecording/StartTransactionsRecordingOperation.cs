using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    public sealed class StartTransactionsRecordingOperation : IMaintenanceOperation
    {
        private readonly string _filePath;

        public StartTransactionsRecordingOperation(string filePath)
        {
            _filePath = filePath;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartTransactionsRecordingCommand(conventions, _filePath);
        }

        private class StartTransactionsRecordingCommand : RavenCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _filePath;

            public StartTransactionsRecordingCommand(DocumentConventions conventions, string filePath)
            {
                EnsureIsNotNullOrEmpty(filePath, nameof(filePath));
                _conventions = conventions;
                _filePath = filePath;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/transactions/start-recording";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var parametersJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new Parameters { File = _filePath }, ctx);
                        await ctx.WriteAsync(stream, parametersJson).ConfigureAwait(false);
                    }, _conventions)
                };
                return request;
            }
        }

        public sealed class Parameters
        {
            public string File { get; set; }
        }
    }
}
