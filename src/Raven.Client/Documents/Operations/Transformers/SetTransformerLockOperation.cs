using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class SetTransformerLockOperation : IAdminOperation
    {
        private readonly string _transformerName;
        private readonly TransformerLockMode _mode;

        public SetTransformerLockOperation(string transformerName, TransformerLockMode mode)
        {
            _transformerName = transformerName ?? throw new ArgumentNullException(nameof(transformerName));
            _mode = mode;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetTransformerLockCommand(_transformerName, _mode);
        }

        private class SetTransformerLockCommand : RavenCommand
        {
            private readonly string _transformerName;
            private readonly TransformerLockMode _mode;

            public SetTransformerLockCommand(string transformerName, TransformerLockMode mode)
            {
                _transformerName = transformerName ?? throw new ArgumentNullException(nameof(transformerName));
                _mode = mode;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers/set-lock?name={Uri.EscapeDataString(_transformerName)}&mode={_mode}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}