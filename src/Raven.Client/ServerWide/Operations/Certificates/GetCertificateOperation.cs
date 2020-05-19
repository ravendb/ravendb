using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class GetCertificateOperation : IServerOperation<CertificateDefinition>
    {
        private readonly string _thumbprint;

        public GetCertificateOperation(string thumbprint)
        {
            _thumbprint = thumbprint ?? throw new ArgumentNullException(nameof(thumbprint));
        }

        public RavenCommand<CertificateDefinition> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCertificateCommand(_thumbprint);
        }

        private class GetCertificateCommand : RavenCommand<CertificateDefinition>
        {
            private readonly string _thumbprint;

            public GetCertificateCommand(string thumbprint)
            {
                _thumbprint = thumbprint ?? throw new ArgumentNullException(nameof(thumbprint));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?thumbprint=" + Uri.EscapeDataString(_thumbprint);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                var results = JsonDeserializationClient.GetCertificatesResponse(response).Results;

                if (results.Length != 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }
        }
    }
}
