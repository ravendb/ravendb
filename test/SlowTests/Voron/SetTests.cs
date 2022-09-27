using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Sets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron;

public class SetTests : StorageTest
{
    private readonly List<long> _data;
    private readonly List<long> _random;

    public SetTests(ITestOutputHelper output) : base(output)
    {
        const int Size = 400_000;
        var diff = new[] { 17, 250, 4828, 28, 12, 3 };
        var random = new Random(231);
        _data = new List<long>();
        long s = 0;
        for (int i = 0; i < Size; i++)
        {
            s += diff[random.Next(diff.Length)];
            _data.Add(s);
        }

        _random = _data.OrderBy(x => random.Next()).ToList();
    }

    private List<long> AllValues(Set set)
    {
        var it = set.Iterate();
        var l = new List<long>();
        if (it.Seek(0) == false)
            return l;
        while (it.MoveNext())
        {
            l.Add(it.Current);
        }

        return l;
    }
    
    [Fact]
    public void CanDeleteRandomLargeNumberOfItemsFromStart()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _random)
            {
                tree.Remove(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenSet("test");
            Assert.Empty(AllValues(tree));
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
            Assert.Equal(1, tree.State.Depth);
        }
    }
    
    [Fact]
    public void CanStoreLargeNumberOfItemsInSequentialOrder()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenSet("test");
            Assert.Equal(_data, AllValues(tree));
        }
    }
    
    [Fact]
    public void CanAddPredictableOffsets_Large()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");

            for (int i = 0; i < 10_000; i++)
            {
                var offset = (i + 100) * 8192;
                for (int j = 0; j < 128; j++)
                {
                    offset += 2;
                    tree.Add(offset);
                }
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenSet("test");
            var it = tree.Iterate();
            Assert.True(it.Seek(0));
            bool movedNext = true;
            for (int i = 0; i < 10_000; i++)
            {
                var offset = (i + 100) * 8192;
                for (int j = 0; j < 128; j++)
                {
                    offset += 2;
                    movedNext = it.MoveNext();
                    Assert.True(movedNext);
                    Assert.Equal(offset, it.Current);
                }
            }

            Assert.False(it.MoveNext());
        }
    }

    [Fact]
    public void CanDeleteLargeNumberOfItemsFromStart()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _data)
            {
                tree.Remove(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenSet("test");
            Assert.Empty(AllValues(tree));
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
            Assert.Equal(1, tree.State.Depth);
        }
    }

    [Fact]
    public void CanStoreLargeNumberOfItemsInRandomlyOrder()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _random)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenSet("test");
            List<long> allValues = AllValues(tree);
            IEnumerable<long> diff = _data.Except(allValues).ToArray();
            Assert.Empty(diff);
            Assert.Equal(_data, allValues);
        }
    }
    
    [Fact]
    public void CanDeleteLargeNumberOfItems()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _random)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _random)
            {
                tree.Remove(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenSet("test");
            Assert.Empty(AllValues(tree));
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
            Assert.Equal(1, tree.State.Depth);
        }
    }
    
    [Fact]
    public void CanDeleteLargeNumberOfItemsFromEnd()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenSet("test");
            for (int i = _data.Count - 1; i >= 0; i--)
            {
                tree.Remove(_data[i]);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenSet("test");
            Assert.Empty(AllValues(tree));
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
            Assert.Equal(1, tree.State.Depth);
        }
    }
}
