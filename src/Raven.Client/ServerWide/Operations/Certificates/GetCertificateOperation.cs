using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class GetCertificateOperation : IServerOperation<CertificateDefinition>
    {
        private readonly string _name;

        public GetCertificateOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand<CertificateDefinition> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCertificateCommand(_name);
        }

        private class GetCertificateCommand : RavenCommand<CertificateDefinition>
        {
            private readonly string _name;

            public GetCertificateCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?name=" + Uri.EscapeDataString(_name);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
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