using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class PutFeatureCertificateOperation : IMaintenanceOperation
    {
        private readonly X509Certificate2 _certificate;
        private readonly string _name;

        public PutFeatureCertificateOperation(string name, X509Certificate2 certificate)
        {
            _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));

            if (certificate.HasPrivateKey == false)
                throw new ArgumentException("The provided certificate must contain the private key.");

            _name = name;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutFeatureCertificateCommand(_name, _certificate);
        }

        private class PutFeatureCertificateCommand : RavenCommand
        {
            private readonly X509Certificate2 _certificate;
            private readonly string _name;

            public PutFeatureCertificateCommand(string name, X509Certificate2 certificate)
            {
                _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
                _name = name;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/certificates/feature";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(CertificateDefinition.Name));
                            writer.WriteString(_name.ToString());
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Certificate));
                            writer.WriteString(Convert.ToBase64String(_certificate.Export(X509ContentType.Pkcs12)));
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Permissions));
                            writer.WriteStartObject();
                            writer.WriteEndObject();
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }
        }
    }
}
