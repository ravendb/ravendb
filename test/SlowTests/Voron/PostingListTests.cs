using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.PostingLists;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron;

public class PostingListTests : StorageTest
{
    private readonly List<long> _data;
    private readonly List<long> _random;

    public PostingListTests(ITestOutputHelper output) : base(output)
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

    private unsafe List<long> AllValues(PostingList postingList)
    {
        var it = postingList.Iterate();
        var l = new List<long>();
        Span<long> buffer = stackalloc long[1024];
        if (it.Seek(0) == false)
            return l;
        while (it.Fill(buffer, out var read))
        {
            for (int i = 0; i < read; i++)
            {
                l.Add(buffer[i]);
            }
        }

        return l;
    }
    
    [Fact]
    public void CanDeleteRandomLargeNumberOfItemsFromStart()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _random)
            {
                tree.Remove(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenPostingList("test");
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
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenPostingList("test");
            Assert.Equal(_data, AllValues(tree));
        }
    }
    
    [Fact]
    public void CanAddPredictableOffsets_Large()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");

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
            var tree = rtx.OpenPostingList("test");
            var it = tree.Iterate();
            Assert.True(it.Seek(0));
            Span<long> buffer = stackalloc long[256];
            var bufIdx = 0;
            Assert.True(it.Fill(buffer, out var read));
            for (int i = 0; i < 10_000; i++)
            {
                var offset = (i + 100) * 8192;
                for (int j = 0; j < 128; j++)
                {
                    offset += 2;
                    if (bufIdx == read)
                    {
                        Assert.True(it.Fill(buffer, out  read));
                        bufIdx = 0;
                    }

                    var current = buffer[bufIdx++]; 
                    Assert.Equal(offset, current);
                }
            }

            Assert.False(it.Fill(buffer, out  read));
        }
    }

    [Fact]
    public void CanDeleteLargeNumberOfItemsFromStart()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _data)
            {
                tree.Remove(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenPostingList("test");
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
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _random)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenPostingList("test");
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
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _random)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _random)
            {
                tree.Remove(i);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenPostingList("test");
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
            var tree = wtx.OpenPostingList("test");
            foreach (long i in _data)
            {
                tree.Add(i);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.OpenPostingList("test");
            for (int i = _data.Count - 1; i >= 0; i--)
            {
                tree.Remove(_data[i]);
            }

            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.OpenPostingList("test");
            Assert.Empty(AllValues(tree));
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
            Assert.Equal(1, tree.State.Depth);
        }
    }
}
