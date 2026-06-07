using System.Security.Cryptography;

namespace Raven.AiAppliance.Channels;

/// <summary>
/// The single random-id recipe for every unguessable public identifier the
/// appliance mints: channel widgetIds (<c>wgt_</c>), embed conversation
/// tokens (<c>cnv_</c>) and pre-minted conversation doc ids (<c>chats/</c>).
/// 128 bits from the crypto RNG rendered as 22 unpadded base64url chars —
/// safe inside doc ids, URLs and HTML attributes without encoding.
/// (RavenDB doc-id lookups are case-insensitive, so the effective keyspace
/// is ~110-115 bits — still far beyond enumerable.)
///
/// SECURITY (A2): this MUST stay on <see cref="RandomNumberGenerator"/>.
/// <c>System.Random</c> is seedable/predictable and would silently make every
/// id enumerable; shape tests cannot detect such a substitution — reviewers must.
/// </summary>
internal static class RandomIds
{
    /// <summary>16 bytes → 24 base64 chars minus the two <c>=</c> pads.</summary>
    internal const int SuffixLength = 22;

    internal static string NewId(string prefix)
    {
        return prefix + Convert.ToBase64String(RandomNumberGenerator.GetBytes(16))
            .TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    /// <summary>
    /// The validate twin of <see cref="NewId"/> — emit and gate share this one
    /// alphabet definition so an encoding change can't silently desync the
    /// embed token check (L2, impl review 2026-06-07). True iff
    /// <paramref name="suffix"/> is exactly <see cref="SuffixLength"/> unpadded
    /// base64url chars. Shape only, not auth: the binding-doc load downstream
    /// is the authority.
    /// </summary>
    internal static bool IsValidSuffix(ReadOnlySpan<char> suffix)
    {
        if (suffix.Length != SuffixLength)
            return false;

        foreach (var c in suffix)
        {
            if (char.IsAsciiLetterOrDigit(c) == false && c != '-' && c != '_')
                return false;
        }

        return true;
    }
}
