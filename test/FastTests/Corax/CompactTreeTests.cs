using System;
using System.Collections.Generic;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CompactTreeTests : StorageTest
{
    public CompactTreeTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanHandleMerges()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");

            for (int i = 0; i < 2000; i++)
            {
                tree.Add(i.ToString(), i);
            }
            
          
            for (int i = 0; i < 2000; i++)
            {
                Assert.True(tree.TryRemove(i.ToString(), out var l));
                Assert.Equal(i, l);
            }

            wtx.Commit();
        }
    }
    
    [Fact]
    public void CanHandlePageSplits()
    {
        var expected = new List<(string,long)>();
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");

            for (int i = 0; i < 2000; i++)
            {
                tree.Add(i.ToString(), i);
                expected.Add((i.ToString(), i));
            }
            
            var entries = tree.AllEntriesIn(tree.State.RootPage);
            // we remove the entry that is the left most in the root entry
            Assert.True(tree.TryRemove(entries[^1].Item1, out _));
            expected.RemoveAll(x => x.Item1 == entries[^1].Item1);
            wtx.Commit();
        }
        expected.Sort((x,y) => string.CompareOrdinal(x.Item1, y.Item1));
          
        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.CompactTreeFor("test");
         
            var it = tree.Iterate();
            it.Reset();
            var items = new List<(string, long)>();
            while (it.MoveNext(out var scope, out var v))
            {
                items.Add((scope.Key.ToString(), v));
            }
            Assert.Equal(expected, items);
        }
    }
    
    [Fact]
    public void CanHandlePageSplitsWithCompression()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");

            for (int i = 0; i < 2000; i++)
            {
                tree.Add("A" + i, i);
            }

            tree._state.NextTrainAt = 1; // force new dictionary generation
            
            wtx.Commit();
        }
        
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");

            for (int i = 0; i < 2000; i++)
            {
                tree.Add("B" + i, i);
            }
            tree._state.NextTrainAt = int.MaxValue;

            wtx.Commit();
        }
        
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");
 
            // All B items are encoded with the new dic, all A items with the old
            // we are removing entries directly in the middle between them, forcing 
            // us to merge pages with different dictionaries
            for (int i = 0; i < 1000; i++)
            {
                Assert.True(tree.TryRemove( "B" + i, out var l));
                Assert.Equal(i, l);
            }
            for (int i = 1000; i < 2000; i++)
            {
                Assert.True(tree.TryRemove( "A" + i, out var l));
                Assert.Equal(i, l);
            }

            wtx.Commit();
        }
    }

    [Fact]
    public void CanAddIterateAndRemove()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");
            
            tree.Add("one", 1);
            tree.Add("two", 2);
            tree.Add("three", 3);
            wtx.Commit();
        }
        
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");
            
            tree.Add("one", 10);
            wtx.Commit();
        }
        
         
        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.CompactTreeFor("test");
            
            Assert.True(tree.TryGetValue("one", out var item));
            Assert.Equal(10, item);
            Assert.True(tree.TryGetValue("three", out  item));
            Assert.Equal(3, item);
            Assert.True(tree.TryGetValue("two", out  item));
            Assert.Equal(2, item);

            var it = tree.Iterate();
            it.Reset();
            var expected = new List<(string, long)> { ("one", 10), ("three", 3), ("two", 2), };
            var items = new List<(string, long)>();
            while (it.MoveNext(out var scope, out var v))
            {
                items.Add((scope.Key.ToString(), v));
            }
            Assert.Equal(expected, items);
        }
        
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");
            
            tree.TryRemove("two", out var old);
            Assert.Equal(2, old);
            wtx.Commit();
        }
    }

    [Theory]
    [InlineDataWithRandomSeed]
    public unsafe void CanEncodeAndDecode(int seed)
    {
        var r = new Random(seed);
        
        var buffer = stackalloc byte[CompactTree.EncodingBufferSize];

        long key = 0;
        long val = 0;
        for (int i = 0; i < 8; i++)
        {
            key = (key << 8) + r.Next(0, 128);
            val = (val << 8) + r.Next(0, 128);
        
            var len = CompactTree.EncodeEntry(key, val, buffer);
            Assert.True(len <= CompactTree.EncodingBufferSize);
            var lenDecoded = CompactTree.DecodeEntry(buffer, out var k, out var v);
        
            Assert.Equal(len, lenDecoded);
            Assert.Equal(key, k);
            Assert.Equal(val, v);
        }
    }
}
