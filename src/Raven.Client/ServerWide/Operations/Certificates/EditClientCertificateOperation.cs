using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    /// <summary>
    /// Allows to edit a client certificate.
    /// </summary>
    public sealed class EditClientCertificateOperation : IServerOperation
    {
        public sealed class Parameters
        {
            public string Thumbprint { get; set; }
            public Dictionary<string, DatabaseAccess> Permissions { get; set; }
            public string Name { get; set; }
            public SecurityClearance Clearance { get; set; }
            public bool Disabled { get; set; }

            // SSO configuration is opt-in: leave these null to keep the existing SSO settings untouched (which is what
            // a regular client-certificate edit, or a Disabled-only toggle on an SSO user, wants). Setting any of them
            // (even to an empty list) fully replaces the stored value - that is how an SSO user's authorizing servers
            // or identifiers can be cleared.
            public List<string> SsoServerPublicKeyPinningHashes { get; set; }
            public bool? AllowAnySsoServer { get; set; }
            public List<SsoIdentifier> SsoIdentifiers { get; set; }
        }

        private readonly string _thumbprint;
        private readonly Dictionary<string, DatabaseAccess> _permissions;
        private readonly string _name;
        private readonly SecurityClearance _clearance;
        private readonly bool _disabled;
        private readonly List<string> _ssoServerPublicKeyPinningHashes;
        private readonly bool? _allowAnySsoServer;
        private readonly List<SsoIdentifier> _ssoIdentifiers;

        /// <inheritdoc cref="EditClientCertificateOperation"/>
        /// <param name="parameters">See <see cref="Parameters"/>.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="parameters"/> argument is null, or any of its required properties are null.</exception>
        public EditClientCertificateOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));
            _name = parameters.Name ?? throw new ArgumentNullException(nameof(parameters.Name));
            _thumbprint = parameters.Thumbprint ?? throw new ArgumentNullException(nameof(parameters.Thumbprint));
            _permissions = parameters.Permissions ?? throw new ArgumentNullException(nameof(parameters.Permissions));
            _clearance = parameters.Clearance;
            _disabled = parameters.Disabled;
            _ssoServerPublicKeyPinningHashes = parameters.SsoServerPublicKeyPinningHashes;
            _allowAnySsoServer = parameters.AllowAnySsoServer;
            _ssoIdentifiers = parameters.SsoIdentifiers;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new EditClientCertificateCommand(conventions, _thumbprint, _name, _permissions, _clearance, _disabled,
                _ssoServerPublicKeyPinningHashes, _allowAnySsoServer, _ssoIdentifiers);
        }

        private sealed class EditClientCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _thumbprint;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly string _name;
            private readonly SecurityClearance _clearance;
            private readonly bool _disabled;
            private readonly List<string> _ssoServerPublicKeyPinningHashes;
            private readonly bool? _allowAnySsoServer;
            private readonly List<SsoIdentifier> _ssoIdentifiers;

            public EditClientCertificateCommand(DocumentConventions conventions, string thumbprint, string name, Dictionary<string, DatabaseAccess> permissions,
                SecurityClearance clearance, bool disabled, List<string> ssoServerPublicKeyPinningHashes, bool? allowAnySsoServer, List<SsoIdentifier> ssoIdentifiers)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _thumbprint = thumbprint;
                _name = name;
                _permissions = permissions;
                _clearance = clearance;
                _disabled = disabled;
                _ssoServerPublicKeyPinningHashes = ssoServerPublicKeyPinningHashes;
                _allowAnySsoServer = allowAnySsoServer;
                _ssoIdentifiers = ssoIdentifiers;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates/edit";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName(nameof(CertificateDefinition.Thumbprint));
                            writer.WriteString(_thumbprint);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(CertificateDefinition.Name));
                            writer.WriteString(_name);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(CertificateDefinition.SecurityClearance));
                            writer.WriteString(_clearance.ToString());
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(CertificateDefinition.Disabled));
                            writer.WriteBool(_disabled);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(CertificateDefinition.Permissions));
                            writer.WriteStartObject();
                            var first = true;
                            foreach (var kvp in _permissions)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;
                                writer.WritePropertyName(kvp.Key);
                                writer.WriteString(kvp.Value.ToString());
                            }
                            writer.WriteEndObject();

                            // The SSO fields are written only when explicitly provided so the server leaves the existing
                            // SSO configuration untouched on a partial edit, and clears it when an empty list is sent.
                            if (_ssoServerPublicKeyPinningHashes != null)
                            {
                                writer.WriteComma();
                                writer.WriteArray(nameof(CertificateDefinition.SsoServerPublicKeyPinningHashes), _ssoServerPublicKeyPinningHashes);
                            }

                            if (_allowAnySsoServer != null)
                            {
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(CertificateDefinition.AllowAnySsoServer));
                                writer.WriteBool(_allowAnySsoServer.Value);
                            }

                            if (_ssoIdentifiers != null)
                            {
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(CertificateDefinition.SsoIdentifiers));
                                writer.WriteStartArray();
                                var firstId = true;
                                foreach (var id in _ssoIdentifiers)
                                {
                                    if (firstId == false)
                                        writer.WriteComma();
                                    firstId = false;

                                    writer.WriteStartObject();
                                    writer.WritePropertyName(nameof(SsoIdentifier.Provider));
                                    writer.WriteString(id.Provider.ToString());
                                    writer.WriteComma();
                                    writer.WritePropertyName(nameof(SsoIdentifier.Identifier));
                                    writer.WriteString(id.Identifier);
                                    if (string.IsNullOrEmpty(id.Domain) == false)
                                    {
                                        writer.WriteComma();
                                        writer.WritePropertyName(nameof(SsoIdentifier.Domain));
                                        writer.WriteString(id.Domain);
                                    }
                                    writer.WriteEndObject();
                                }
                                writer.WriteEndArray();
                            }

                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
