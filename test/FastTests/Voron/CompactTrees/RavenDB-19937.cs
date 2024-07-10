using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.CompactTrees;

public class RavenDB_19937 : CompactTreeReplayTest
{
    public RavenDB_19937(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData("RavenDB-19937.replay")]
    [InlineData("RavenDB-19956.replay")]
    public unsafe void CorruptionWithAddAndRemoveValues(string filename)
    {
        foreach (var terms in ReadTermsFromResource(filename))
        {
            using var wtx = Env.WriteTransaction();
            CompactTree tree = wtx.CompactTreeFor($"{filename}");
            for( int i = 0; i < terms.Count; i++)
            {
                var term = terms[i];
                var action = term[0];

                string key;
                switch (action)
                {
                    case '+':
                        int pipeIndex = term.LastIndexOf('|');
                        key = term[1..pipeIndex];
                        var value = long.Parse(term[(pipeIndex + 1)..]);
                        tree.Add(key, value);
                        Assert.True(tree.TryGetValue(key, out var foundId));
                        Assert.Equal(value, foundId);
                        break;
                    case '-':
                        key = term[1..];
                        tree.TryRemove(key, out var old);
                        break;
                }

                tree.VerifyOrderOfElements();
            }
            wtx.Commit();
        }

        {
            using var rtx = Env.ReadTransaction();

            CompactTree tree = rtx.CompactTreeFor($"{filename}");

            tree.VerifyOrderOfElements();
            foreach (long page in tree.AllPages())
            {
                var state = new Lookup<CompactTree.CompactKeyLookup>.CursorState() { Page = rtx.LowLevelTransaction.GetPage(page), };
                Assert.Equal(state.ComputeFreeSpace(), state.Header->FreeSpace);
            }
        }
    }
}
