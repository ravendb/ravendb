using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class GetCertificatesMetadataOperation : IServerOperation<CertificateMetadata[]>
    {
        private readonly string _name;

        public GetCertificatesMetadataOperation(string name = null)
        {
            _name = name;
        }

        public RavenCommand<CertificateMetadata[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCertificatesMetadataCommand(_name);
        }

        private class GetCertificatesMetadataCommand : RavenCommand<CertificateMetadata[]>
        {
            private readonly string _name;

            public GetCertificatesMetadataCommand(string name)
            {
                _name = name;
            }
            
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var path = new StringBuilder(node.Url).Append("/admin/certificates?metadataOnly=true");
                if (string.IsNullOrEmpty(_name) == false)
                    path.Append("&name=").Append(Uri.EscapeDataString(_name));

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

                Result = JsonDeserializationClient.GetCertificatesMetadataResponse(response).Results;
            }
        }
    }
}