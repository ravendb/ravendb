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
        private readonly string _certificateBase64;
        private readonly HashSet<string> _permissions;
        private readonly bool _serverAdmin;
        private readonly string _password;

        public PutClientCertificateOperation(string certificateBase64, IEnumerable<string> permissions, bool serverAdmin = false, string password = null)
        {
            _certificateBase64 = certificateBase64 ?? throw new ArgumentNullException(nameof(_certificateBase64));
            _permissions = permissions != null ? new HashSet<string>(permissions) : throw new ArgumentNullException(nameof(permissions));
            _serverAdmin = serverAdmin;
            _password = password;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutClientCertificateCommand(context, _certificateBase64, _permissions, _serverAdmin, _password);
        }

        private class PutClientCertificateCommand : RavenCommand
        {
            private readonly string _certificateBase64;
            private readonly HashSet<string> _permissions;
            private readonly bool _serverAdmin;
            private readonly string _password;
            private readonly JsonOperationContext _context;

            public PutClientCertificateCommand(JsonOperationContext context, string certificateBase64, HashSet<string> permissions, bool serverAdmin = false, string password = null)
            {
                _certificateBase64 = certificateBase64 ?? throw new ArgumentNullException(nameof(certificateBase64));
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
                _serverAdmin = serverAdmin;
                _password = password;
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
                            writer.WriteString(_certificateBase64);
                            writer.WriteComma();
                            writer.WritePropertyName("ServerAdmin");
                            writer.WriteBool(_serverAdmin);
                            writer.WriteComma();

                            if (_password != null)
                            {
                                writer.WritePropertyName("Password");
                                writer.WriteString(_password.ToString());
                                writer.WriteComma();
                            }

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