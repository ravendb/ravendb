﻿using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests.Voron;
using Lucene.Net.Search.Function;
using Raven.Client.Documents.Linq.Indexing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Util.PFor;
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
                var list = tx.LookupFor<Int64LookupKey>("test");
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
                var list = tx.LookupFor<Int64LookupKey>("test");
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
        var list = tx.LookupFor<Int64LookupKey>("test");

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
    
    [Fact]
    public unsafe void ShouldNotCrash()
    {
        var buffer = Convert.FromBase64String(
            "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");

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        // we need to setup the allocation pattern in a very specific manner to trigger this
        using var aa = bsc.Allocate(4000, out _);
        using var ab = bsc.Allocate(4000, out _);
        using var ac = bsc.Allocate(4000, out _);
        
        // so the small entry for the native list will come before the earlier allocation of the buffer
        var small = bsc.Allocate(128, out _);
        var big = bsc.Allocate(4096, out _);
        small.Dispose();
        big.Dispose();

        var output = stackalloc long[2048];
        fixed (byte* input = buffer)
        {
            int existingCount = 0;
            var reader = new FastPForBufferedReader(bsc, input, buffer.Length);
            while (true)
            {
                var read = reader.Fill(output + existingCount, 2048 - existingCount);
                existingCount += read;
                if (read == 0)
                {
                    reader.Dispose();
                    break;
                }
            }
        }
    }
}