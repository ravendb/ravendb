using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class PutCertificateOperation : IServerOperation
    {
        private readonly string _name;
        private readonly CertificateDefinition _certificate;

        public PutCertificateOperation(string name, CertificateDefinition certificate)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutCertificateCommand(conventions, context, _name, _certificate);
        }

        private class PutCertificateCommand : RavenCommand
        {
            private readonly JsonOperationContext _context;
            private readonly string _name;
            private readonly BlittableJsonReaderObject _certificate;

            public PutCertificateCommand(DocumentConventions conventions, JsonOperationContext context, string name, CertificateDefinition certificate)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (certificate == null)
                    throw new ArgumentNullException(nameof(certificate));

                _context = context ?? throw new ArgumentNullException(nameof(context));
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _certificate = EntityToBlittable.ConvertEntityToBlittable(certificate, conventions, context);
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?name=" + Uri.EscapeDataString(_name);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _certificate);
                    })
                };
            }
        }
    }
}