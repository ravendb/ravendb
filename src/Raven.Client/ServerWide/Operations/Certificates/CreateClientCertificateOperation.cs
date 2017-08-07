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
        private readonly bool _serverAdmin;
        private readonly string _password;

        public CreateClientCertificateOperation(string name, Dictionary<string, DatabaseAccess> permissions, bool serverAdmin = false, string password = null)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
            _serverAdmin = serverAdmin;
            _password = password;
        }

        public RavenCommand<CertificateRawData> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateClientCertificateCommand(context, _name, _permissions, _serverAdmin, _password);
        }

        private class CreateClientCertificateCommand : RavenCommand<CertificateRawData>
        {
            private readonly string _name;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly bool _serverAdmin;
            private readonly string _password;
            private readonly JsonOperationContext _context;

            public CreateClientCertificateCommand(JsonOperationContext context, string name, Dictionary<string, DatabaseAccess> permissions, bool serverAdmin = false, string password = null)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
                _serverAdmin = serverAdmin;
                _password = password;
                ResponseType = RavenCommandResponseType.Raw;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                request.Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(_context, stream))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName("Name");
                        writer.WriteString(_name.ToString());
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