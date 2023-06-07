using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests.Voron;
using Lucene.Net.Search.Function;
using Raven.Client.Documents.Linq.Indexing;
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
    public unsafe void CanManagePageSplit2()
    {
        var reader = new StreamReader(typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs.page-base64.txt"));
        while (true)
        {
            string line = reader.ReadLine();
            if (line == null) break;
            
            // we are reproducing a scenario here in the worst possible way, but simply *copying* the raw bytes into the page
            // this is done because we aren't sure _how_ we gotten to this state. Note that changing the behavior / structure of lookup may mess up
            // this test, but that is probably not likely, since the format should be backward compatible. 
            byte[] data = Convert.FromBase64String(line);
            long rootPage;
            using( var tx = Env.WriteTransaction())
            {
                var list = tx.LookupFor<long>("test");
                rootPage = list.State.RootPage;
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // raw copying of the data to the page, bypassing the actual logic
                fixed (byte* b = data)
                {
                    Page modifyPage = tx.LowLevelTransaction.ModifyPage(rootPage);
                    Unsafe.CopyBlock(modifyPage.Pointer,b, 8192);
                    modifyPage.PageNumber = rootPage; // we overwrote that
                    tx.Commit();
                }
            }
            
            var parts = reader.ReadLine()!.Split(' ');

            using( var tx = Env.WriteTransaction())
            {
                var list = tx.LookupFor<long>("test");
                list.Add(long.Parse(parts[0]), long.Parse(parts[1]));
                tx.Commit();
            }
        }
    }

    
    [Fact]
    public void CanManagePageSplit()
    {
        var reader = new StreamReader(typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs.PostListSplit.txt"));
        using var tx = Env.WriteTransaction();
        var list = tx.LookupFor<long>("test");

        while (true)
        {
            var l = reader.ReadLine();
            if (l == null) break;
            string[] parts = l.Split(' ');
            list.Add(long.Parse(parts[0]), long.Parse(parts[1]));
        }
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
            var actual = new long[PostingListLeafPage.GetNextValidBufferSize(vals.Length)];
            var it = list.Iterate();
            Assert.True(it.Fill(actual, out var total));
            Assert.Equal(total, vals.Length);
            Assert.Equal(vals, actual.Take(total));
            Assert.False(it.Fill(actual, out  total));
        }
    }
}
