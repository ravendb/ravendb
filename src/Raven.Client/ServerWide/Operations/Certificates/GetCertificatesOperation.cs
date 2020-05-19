using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class GetCertificatesOperation : IServerOperation<CertificateDefinition[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetCertificatesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<CertificateDefinition[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCertificatesCommand(_start, _pageSize);
        }

        private class GetCertificatesCommand : RavenCommand<CertificateDefinition[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetCertificatesCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?start={_start}&pageSize={_pageSize}";

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

                Result = JsonDeserializationClient.GetCertificatesResponse(response).Results;
            }
        }
    }
}