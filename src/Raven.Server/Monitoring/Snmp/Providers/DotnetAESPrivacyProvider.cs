// Native .NET AES-128 privacy provider
// Inherits from the native .NET base provider.

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Security;

namespace Raven.Server.Monitoring.Snmp.Providers;

/// <summary>
/// Privacy provider for AES 128 using native .NET cryptography.
/// </summary>
public sealed class DotnetAESPrivacyProvider : DotnetAESPrivacyProviderBase
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DotnetAESPrivacyProvider"/> class.
    /// </summary>
    /// <param name="phrase">The phrase.</param>
    /// <param name="auth">The authentication provider.</param>
    public DotnetAESPrivacyProvider(OctetString phrase, IAuthenticationProvider auth)
        : base(16, phrase, auth) // AES-128 uses a 16-byte (128-bit) key.
    {
    }

    /// <summary>
    /// Returns a string that represents this object.
    /// </summary>
    public override string ToString() => "AES 128 (native) privacy provider";
}
