using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Voron;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.CompactTrees;

public class RavenDB_19703 : CompactTreeReplayTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public RavenDB_19703(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
    {
        _testOutputHelper = testOutputHelper;
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData("RavenDB-19703.replay")]
    public unsafe void AddAndRemoveValues(string filename)
    {
        foreach (var terms in ReadTermsFromResource(filename))
        {
            using var wtx = Env.WriteTransaction();
            CompactTree tree = wtx.CompactTreeFor($"{filename}");
            foreach (var term in terms)
            {
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

                tree.Verify();
                tree.VerifyOrderOfElements();
            }
            wtx.Commit();
        }

        {
            using var rtx = Env.ReadTransaction();

            CompactTree tree = rtx.CompactTreeFor($"{filename}");

            tree.Verify();
            tree.VerifyOrderOfElements();
            foreach (long page in tree.AllPages())
            {
                var state = new CompactTree.CursorState { Page = rtx.LowLevelTransaction.GetPage(page), };
                Assert.Equal(state.ComputeFreeSpace(), state.Header->FreeSpace);
            }
        }
    }
}
