using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    /// <summary>
    /// Allows to register a client certificate.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Operations.ServerOperations.PutClientCertificateOperation"/>
    public sealed class PutClientCertificateOperation : IServerOperation
    {
        private readonly X509Certificate2 _certificate;
        private readonly Dictionary<string, DatabaseAccess> _permissions;
        private readonly string _name;
        private readonly SecurityClearance _clearance;
        
        public string TwoFactorAuthenticationKey { get; set; }

        /// <inheritdoc cref="PutClientCertificateOperation"/>
        /// <param name="name">Certificate name.</param>
        /// <param name="certificate">Client certificate to be registered.</param>
        /// <param name="permissions">Dictionary mapping databases (by name) to access level.</param>
        /// <param name="clearance">Access level (role) assigned to certificate.</param>
        /// <exception cref="ArgumentNullException">Thrown when either <paramref name="name"/>, <paramref name="certificate"/> or <paramref name="permissions"/> are null.</exception>
        public PutClientCertificateOperation(string name, X509Certificate2 certificate, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance)
        {
            _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
            _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _clearance = clearance;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutClientCertificateCommand(conventions, _name, _certificate, _permissions, _clearance, TwoFactorAuthenticationKey);
        }

        private sealed class PutClientCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly X509Certificate2 _certificate;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly string _name;
            private readonly SecurityClearance _clearance;
            private readonly string _twoFactorAuthenticationKey;

            public PutClientCertificateCommand(DocumentConventions conventions, string name, X509Certificate2 certificate, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance,
                string twoFactorAuthenticationKey)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
                _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
                _name = name;
                _clearance = clearance;
                _twoFactorAuthenticationKey = twoFactorAuthenticationKey;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(CertificateDefinition.Name));
                            writer.WriteString(_name);
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.Certificate));
                            writer.WriteString(Convert.ToBase64String(_certificate.Export(X509ContentType.Cert)));
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(CertificateDefinition.SecurityClearance));
                            writer.WriteString(_clearance.ToString());
                            writer.WriteComma();
                            if (_twoFactorAuthenticationKey != null)
                            {
                                writer.WritePropertyName(nameof(TwoFactorAuthenticationKey));
                                writer.WriteString(_twoFactorAuthenticationKey);
                                writer.WriteComma();
                            }
                            writer.WritePropertyName(nameof(CertificateDefinition.Permissions));
                            writer.WriteStartObject();
                            bool first = true;
                            foreach (var kvp in _permissions)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;

                                writer.WriteString(kvp.Key);
                                writer.WriteComma();
                                writer.WriteString(kvp.Value.ToString());
                            }

                            writer.WriteEndObject();
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
