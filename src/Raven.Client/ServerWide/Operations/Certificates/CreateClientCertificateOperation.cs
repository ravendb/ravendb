using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class CreateClientCertificateOperation : IServerOperation<CertificateRawData>
    {
        private readonly string _name;
        private readonly Dictionary<string, DatabaseAccess> _permissions;
        private readonly SecurityClearance _clearance;
        private readonly string _password;

        public CreateClientCertificateOperation(string name, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance, string password = null)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
            _clearance = clearance;
            _password = password;
        }

        public RavenCommand<CertificateRawData> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateClientCertificateCommand( _name, _permissions, _clearance, _password);
        }

        private class CreateClientCertificateCommand : RavenCommand<CertificateRawData>
        {
            private readonly string _name;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly SecurityClearance _clearance;
            private readonly string _password;

            public CreateClientCertificateCommand(string name, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance, string password = null)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
                _clearance = clearance;
                _password = password;
                ResponseType = RavenCommandResponseType.Raw;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                request.Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName("Name");
                        writer.WriteString(_name.ToString());
                        writer.WriteComma();
                        writer.WritePropertyName("SecurityClearance");
                        writer.WriteString(_clearance.ToString());
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
                        foreach (var kvp in _permissions)
                        {
                            if (first == false)
                                writer.WriteComma();
                            first = false;

                            writer.WriteStartObject();
                            writer.WritePropertyName("Database");
                            writer.WriteString(kvp.Key);
                            writer.WriteComma();
                            writer.WritePropertyName("Access");
                            writer.WriteString(kvp.Value == DatabaseAccess.ReadWrite ? nameof(DatabaseAccess.ReadWrite) : nameof(DatabaseAccess.Admin));
                            writer.WriteEndObject();
                        }
                        writer.WriteEndArray();

                        writer.WriteEndObject();
                    }
                });

                return request;
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                if (response == null)
                    return;

                var ms = new MemoryStream();
                stream.CopyTo(ms);

                Result = new CertificateRawData
                {
                    RawData = ms.ToArray()
                };
            }
        }
    }
}
