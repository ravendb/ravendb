using System.Text.RegularExpressions;
using FastTests;
using Raven.AiAppliance.Channels;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// RavenDB-26700 (auth follow-up) — the single random-id recipe shared by
/// widgetIds (<c>wgt_</c>), conversation tokens (<c>cnv_</c>) and pre-minted
/// conversation ids (<c>chats/</c>). 128 bits of crypto randomness rendered
/// as 22 unpadded base64url chars; unguessability is the security property
/// every caller relies on (A2).
/// </summary>
public class RandomIdsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("wgt_")]
    [InlineData("cnv_")]
    [InlineData("chats/")]
    public void NewId_is_prefix_plus_22_base64url_chars(string prefix)
    {
        string id = RandomIds.NewId(prefix);

        Assert.StartsWith(prefix, id, StringComparison.Ordinal);

        string suffix = id[prefix.Length..];
        Assert.Equal(RandomIds.SuffixLength, suffix.Length);
        // base64url alphabet only — no '+', '/', '=' padding, and nothing
        // (like '|') that RavenDB doc-id machinery treats specially.
        Assert.Matches(new Regex("^[A-Za-z0-9_-]{22}$"), suffix);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void NewId_is_unique_across_calls()
    {
        HashSet<string> seen = new(StringComparer.Ordinal);
        for (int i = 0; i < 1024; i++)
        {
            Assert.True(seen.Add(RandomIds.NewId("cnv_")), $"duplicate id generated on iteration {i}");
        }
    }
}
