using System;
using System.Linq;
using Corax.Indexing;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Utils;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax.Bugs;

public class TermMatchTests : StorageTest
{
    public TermMatchTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void ScalarTermMatchPostingListAndWithTest()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");
            foreach (var i in Enumerable.Range(1,1589)) 
                tree.Add(EntryIdEncodings.Encode(i, 1, TermIdMask.Single));
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            using var indexSearcher = new IndexSearcher(rtx, null);
            var tree = rtx.OpenPostingList("test");
            var termMatch = TermMatch.YieldSet(indexSearcher, rtx.LowLevelTransaction.Allocator, tree, 1, false, false);
            var result = termMatch.AndWith([24L], 1);
            Assert.Equal(1, result);
        }
        
        using (var rtx = Env.ReadTransaction())
        {
            using var indexSearcher = new IndexSearcher(rtx, null);
            var tree = rtx.OpenPostingList("test");
            var termMatch = TermMatch.YieldSet(indexSearcher, rtx.LowLevelTransaction.Allocator, tree, 1, false, false);
            var matches = ((long[])[24L, 1111L, 1589L, 2024L]).AsSpan();
            var result = termMatch.AndWith(matches, 4);
            Assert.Equal(24L, matches[0]);
            Assert.Equal(1111L, matches[1]);
            Assert.Equal(1589L, matches[2]);
            Assert.Equal(3, result);
        }

        using (var rtx = Env.ReadTransaction())
        {
            using var indexSearcher = new IndexSearcher(rtx, null);
            var tree = rtx.OpenPostingList("test");

            var termMatch = TermMatch.YieldSet(indexSearcher, rtx.LowLevelTransaction.Allocator, tree, 1, false, false);
            var matches = ((long[])[64_000L, 64_0001L, 64_0002L, 64_0003L]).AsSpan();
            var result = termMatch.AndWith(matches, 4);
            Assert.Equal(0, result);
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList(nameof(ScalarTermMatchPostingListAndWithTest));
            foreach (var i in Enumerable.Range(64_000,2048)) 
                tree.Add(EntryIdEncodings.Encode(i, 1, TermIdMask.Single));
            wtx.Commit();
        }
        
        using (var rtx = Env.ReadTransaction())
        {
            using var indexSearcher = new IndexSearcher(rtx, null);
            var tree = rtx.OpenPostingList(nameof(ScalarTermMatchPostingListAndWithTest));
            var termMatch = TermMatch.YieldSet(indexSearcher, rtx.LowLevelTransaction.Allocator, tree, 1, false, false);
            var matches = ((long[])[24L, 1111L, 1589L, 2024L]).AsSpan();
            var result = termMatch.AndWith(matches, 4);
            Assert.Equal(result, 0);
        }
    }
}
