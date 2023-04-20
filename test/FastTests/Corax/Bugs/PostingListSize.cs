using FastTests.Voron;
using Lucene.Net.Search.Function;
using Voron;
using Voron.Data.PostingLists;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class PostingListSize : StorageTest
{
    public PostingListSize(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void PostingListSizeWouldBeReasonable()
    {
        var vals = new long[]
        {
            96086203228164, 98233082920964, 98233200267268, 98233351409668, 98233409961988, 98233477046276, 98241127481348, 98260245262340, 98298019155972,
            100924106416132
        };
        
        {
            using var tx = Env.WriteTransaction();
            var list = tx.OpenPostingList("test");

            
            list.Add(vals);

            tx.Commit();
        }

        {
            using var tx = Env.ReadTransaction();
            var list = tx.OpenPostingList("test");
            Assert.Equal(1, list.State.LeafPages);
            Assert.Equal(0, list.State.BranchPages);
        }
         
        {
            using var tx = Env.ReadTransaction();
            var list = tx.OpenPostingList("test");
            var actual = new long[vals.Length];
            var it = list.Iterate();
            Assert.True(it.Fill(actual, out var total));
            Assert.Equal(total, vals.Length);
            Assert.Equal(vals, actual);
            Assert.False(it.Fill(actual, out  total));
        }
    }
}
