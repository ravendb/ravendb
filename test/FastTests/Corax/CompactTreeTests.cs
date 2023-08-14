using System;
using System.Collections.Generic;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
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
            
            var entries = tree.AllEntriesIn(tree.RootPage);
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
            while (it.MoveNext(out var key, out var v, out _))
            {
                items.Add((key.ToString(), v));
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
            
            wtx.Commit();
        }
        
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");

            for (int i = 0; i < 2000; i++)
            {
                tree.Add("B" + i, i);
            }

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
    public void CanProperlyResetIterator()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor(nameof(CanProperlyResetIterator));
            tree.Add("aaaa", 1 << 2);
            tree.Add("aaab", 2 << 2);
            tree.Add("dddd", 3 << 2);
            tree.Add("eeee", 4 << 2);

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var compactTreeFor = rtx.CompactTreeFor(nameof(CanProperlyResetIterator));
            var forwardIterator = compactTreeFor.Iterate();
            forwardIterator.Seek("dddd");
            forwardIterator.Reset();
            List<string> keys = new();
            while (forwardIterator.MoveNext(out CompactKey compactKey, out long _, out _))
            {
                keys.Add(compactKey.ToString());
            }

            Assert.Contains("aaaa", keys);
            Assert.Contains("aaab", keys);
            Assert.Contains("dddd", keys);
            Assert.Contains("eeee", keys);
        }

        using (var rtx = Env.ReadTransaction())
        {
            //backward
            var compactTreeFor = rtx.CompactTreeFor(nameof(CanProperlyResetIterator));
            var backwardIterator = compactTreeFor.Iterate<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>();
            backwardIterator.Seek("aaab");
            backwardIterator.Reset();
            List<string> keys = new();
            while (backwardIterator.MoveNext(out CompactKey compactKey, out long _, out _))
                keys.Add(compactKey.ToString());
            
            Assert.Contains("aaaa", keys);
            Assert.Contains("aaab", keys);
            Assert.Contains("dddd", keys);
            Assert.Contains("eeee", keys);
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
            while (it.MoveNext(out var key, out var v, out _))
            {
                items.Add((key.ToString(), v));
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
}
