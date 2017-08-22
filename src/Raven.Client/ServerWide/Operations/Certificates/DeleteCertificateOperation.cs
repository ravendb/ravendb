using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class DeleteCertificateOperation : IServerOperation
    {
        private readonly string _thumbprint;

        public DeleteCertificateOperation(string thumbprint)
        {
            _thumbprint = thumbprint ?? throw new ArgumentNullException(nameof(thumbprint));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteCertificateCommand(_thumbprint);
        }

        private class DeleteCertificateCommand : RavenCommand
        {
            private readonly string _thumbprint;

            public DeleteCertificateCommand(string thumbprint)
            {
                _thumbprint = thumbprint ?? throw new ArgumentNullException(nameof(thumbprint));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?thumbprint=" + Uri.EscapeDataString(_thumbprint);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}
