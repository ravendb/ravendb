using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class ReplaceClusterCertificateOperation : IServerOperation
    {
        private readonly byte[] _certBytes;
        private readonly string _name;
        private readonly bool _replaceImmediately;

        public ReplaceClusterCertificateOperation(string name, byte[] certBytes, bool replaceImmediately)
        {
            _certBytes = certBytes ?? throw new ArgumentNullException(nameof(certBytes));
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _replaceImmediately = replaceImmediately;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ReplaceClusterCertificateCommand(_name, _certBytes, _replaceImmediately);
        }

        private class ReplaceClusterCertificateCommand : RavenCommand
        {
            private readonly byte[] _certBytes;
            private readonly string _name;
            private readonly bool _replaceImmediately;

            public ReplaceClusterCertificateCommand(string name, byte[] certBytes, bool replaceImmediately)
            {
                _certBytes = certBytes ?? throw new ArgumentNullException(nameof(certBytes));
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _replaceImmediately = replaceImmediately;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates/replace-cluster-cert?replaceImmediately={_replaceImmediately}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName(nameof(CertificateDefinition.Name));
                            writer.WriteString(_name.ToString());
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Certificate));
                            writer.WriteString(Convert.ToBase64String(_certBytes)); // keep the private key -> this is a server cert

                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }
        }
    }
}
