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
                // SSO user entry: no actual certificate, keyed by the SSO user identifier stored in Thumbprint
                if (string.IsNullOrEmpty(certificateDefinition.Thumbprint))
                    throw new InvalidOperationException("Cannot store an SSO user entry without a thumbprint (should be set to the SSO user identifier).");
                if (string.IsNullOrEmpty(certificateDefinition.PublicKeyPinningHash))
                    throw new InvalidOperationException("Cannot store an SSO user entry without a public key pinning hash (should be a generated GUID).");
                if (string.IsNullOrEmpty(certificateDefinition.Name))
                    throw new InvalidOperationException("Cannot store an SSO user entry without a name.");
                if (certificateDefinition.AllowAnySsoServer == false &&
                    (certificateDefinition.SsoServerPublicKeyPinningHashes == null || certificateDefinition.SsoServerPublicKeyPinningHashes.Count == 0))
                    throw new InvalidOperationException("Cannot store an SSO user entry without at least one SSO server public key pinning hash (or set AllowAnySsoServer = true).");
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
