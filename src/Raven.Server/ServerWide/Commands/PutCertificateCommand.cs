using System;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutCertificateCommand : PutValueCommand<CertificateDefinition>
    {
        public string PublicKeyPinningHash;
        public string TwoFactorAuthenticationKey;

        public PutCertificateCommand()
        {
            // for deserialization
        }

        public PutCertificateCommand(string name, CertificateDefinition value, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = name;
            Value = value;
            PublicKeyPinningHash = value.PublicKeyPinningHash;

            ValidateCertificateDefinition(value);
        }

        public static void ValidateCertificateDefinition(CertificateDefinition certificateDefinition)
        {
            if (certificateDefinition.Usage == CertificateUsage.SsoClient)
            {
                if (string.IsNullOrEmpty(certificateDefinition.Thumbprint))
                    throw new InvalidOperationException("Cannot store an SSO user entry without a thumbprint (should be a generated GUID).");
                if (string.IsNullOrEmpty(certificateDefinition.PublicKeyPinningHash))
                    throw new InvalidOperationException("Cannot store an SSO user entry without a public key pinning hash (should be a generated GUID).");
                if (string.IsNullOrEmpty(certificateDefinition.Name))
                    throw new InvalidOperationException("Cannot store an SSO user entry without a display name.");
                if (certificateDefinition.AllowAnySsoServer == false &&
                    (certificateDefinition.SsoServerPublicKeyPinningHashes == null || certificateDefinition.SsoServerPublicKeyPinningHashes.Count == 0))
                    throw new InvalidOperationException("Cannot store an SSO user entry without at least one SSO server public key pinning hash (or set AllowAnySsoServer = true).");
                if (certificateDefinition.SsoIdentifiers == null || certificateDefinition.SsoIdentifiers.Count == 0)
                    throw new InvalidOperationException("Cannot store an SSO user entry without at least one SSO identifier.");
                foreach (var id in certificateDefinition.SsoIdentifiers)
                {
                    if (string.IsNullOrWhiteSpace(id.Identifier))
                        throw new InvalidOperationException("Each SSO identifier must have a non-empty Identifier value.");
                    if (id.Provider != SsoProvider.Windows && string.IsNullOrEmpty(id.Domain) == false)
                        throw new InvalidOperationException($"SSO identifier for provider '{id.Provider}' must not have a Domain (Domain is only valid for Windows).");
                }
                return;
            }

            if (string.IsNullOrEmpty(certificateDefinition.Certificate))
                throw new InvalidOperationException("Cannot store a certificate definition without the actual certificate!");
            if (string.IsNullOrEmpty(certificateDefinition.Thumbprint))
                throw new InvalidOperationException("Cannot store a certificate without a thumbprint.");
            if (string.IsNullOrEmpty(certificateDefinition.PublicKeyPinningHash))
                throw new InvalidOperationException("Cannot store a certificate without a PublicKeyPinningHash.");
            if (string.IsNullOrEmpty(certificateDefinition.Name))
                throw new InvalidOperationException("Cannot store a certificate without a name.");
            if (certificateDefinition.NotAfter == null)
                throw new InvalidOperationException("Cannot store a certificate without an expiration date.");
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(Value)] = Value?.ToJson();
            djv[nameof(TwoFactorAuthenticationKey)] = TwoFactorAuthenticationKey;
            djv[nameof(PublicKeyPinningHash)] = PublicKeyPinningHash;
            return djv;
        }

        public override DynamicJsonValue ValueToJson()
        {
            var djv = Value?.ToJson();
            if (djv == null)
                return null;
            if (string.IsNullOrEmpty(TwoFactorAuthenticationKey) == false)
            {
                djv[nameof(TwoFactorAuthenticationKey)] = TwoFactorAuthenticationKey;
            }
            return djv;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
