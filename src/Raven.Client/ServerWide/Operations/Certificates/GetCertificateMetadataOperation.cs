using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class GetCertificateMetadataOperation : IServerOperation<CertificateMetadata>
    {
        private readonly string _thumbprint;

        public GetCertificateMetadataOperation(string thumbprint)
        {
            _thumbprint = thumbprint ?? throw new ArgumentNullException(nameof(thumbprint));
        }

        public RavenCommand<CertificateMetadata> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCertificateMetadataCommand(_thumbprint);
        }

        private class GetCertificateMetadataCommand : RavenCommand<CertificateMetadata>
        {
            private readonly string _thumbprint;

            public GetCertificateMetadataCommand(string thumbprint)
            {
                _thumbprint = thumbprint;
            }
            
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var path = new StringBuilder(node.Url)
                    .Append("/admin/certificates?thumbprint=")
                    .Append(Uri.EscapeDataString(_thumbprint))
                    .Append("&metadataOnly=true");

                url = path.ToString();
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

                var results = JsonDeserializationClient.GetCertificatesMetadataResponse(response).Results;

                if (results.Length != 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }
        }
    }
}
