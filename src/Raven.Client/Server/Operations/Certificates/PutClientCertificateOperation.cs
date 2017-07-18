using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.Certificates
{
    public class PutClientCertificateOperation : IServerOperation
    {
        private readonly X509Certificate2 _certificate;
        private readonly HashSet<string> _permissions;
        private readonly bool _serverAdmin;

        public PutClientCertificateOperation(X509Certificate2 certificate, IEnumerable<string> permissions, bool serverAdmin = false)
        {
            _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
            _permissions = permissions != null ? new HashSet<string>(permissions) : throw new ArgumentNullException(nameof(permissions));
            _serverAdmin = serverAdmin;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutClientCertificateCommand(context, _certificate, _permissions, _serverAdmin);
        }

        private class PutClientCertificateCommand : RavenCommand
        {
            private readonly X509Certificate2 _certificate;
            private readonly HashSet<string> _permissions;
            private readonly bool _serverAdmin;
            private readonly JsonOperationContext _context;

            public PutClientCertificateCommand(JsonOperationContext context, X509Certificate2 certificate, HashSet<string> permissions, bool serverAdmin = false)
            {
                _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
                _serverAdmin = serverAdmin;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName("Certificate");
                            writer.WriteString(Convert.ToBase64String(_certificate.Export(X509ContentType.Cert)));
                            writer.WriteComma();
                            writer.WritePropertyName("ServerAdmin");
                            writer.WriteBool(_serverAdmin);
                            writer.WriteComma();
                            writer.WritePropertyName("Permissions");
                            writer.WriteStartArray();
                            bool first = true;
                            foreach (var permission in _permissions)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;

                                writer.WriteString(permission);
                            }
                            writer.WriteEndArray();

                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }
        }
    }
}