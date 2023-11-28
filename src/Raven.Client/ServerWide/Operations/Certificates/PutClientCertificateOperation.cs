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
    public class PutClientCertificateOperation : IServerOperation
    {
        private readonly X509Certificate2 _certificate;
        private readonly Dictionary<string, DatabaseAccess> _permissions;
        private readonly string _name;
        private readonly SecurityClearance _clearance;
        
        public string TwoFactorAuthenticationKey { get; set; }
        public TimeSpan TwoFactorAuthenticationValidityPeriod { get; set; } = TimeSpan.FromHours(2);

        public PutClientCertificateOperation(string name, X509Certificate2 certificate, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance)
        {
            _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
            _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _clearance = clearance;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutClientCertificateCommand(_name, _certificate, _permissions, _clearance, TwoFactorAuthenticationKey, TwoFactorAuthenticationValidityPeriod);
        }

        private class PutClientCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly X509Certificate2 _certificate;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly string _name;
            private readonly SecurityClearance _clearance;
            private readonly string _twoFactorAuthenticationKey;
            private readonly TimeSpan _twoFactorAuthenticationValidityPeriod;

            public PutClientCertificateCommand(string name, X509Certificate2 certificate, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance,
                string twoFactorAuthenticationKey, TimeSpan twoFactorAuthenticationValidityPeriod)
            {
                _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
                _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
                _name = name;
                _clearance = clearance;
                _twoFactorAuthenticationKey = twoFactorAuthenticationKey;
                _twoFactorAuthenticationValidityPeriod = twoFactorAuthenticationValidityPeriod;
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
                                writer.WritePropertyName(nameof(TwoFactorAuthenticationValidityPeriod));
                                writer.WriteString(_twoFactorAuthenticationValidityPeriod.ToString("c", CultureInfo.InvariantCulture));
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
                    })
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
