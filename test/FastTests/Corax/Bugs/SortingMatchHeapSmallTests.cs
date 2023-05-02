using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Corax.Mappings;
using Corax.Queries;
using Google.Protobuf.WellKnownTypes;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using static Corax.Queries.SortingMatch;

namespace FastTests.Corax.Bugs;

public unsafe class SortingMatchHeapSmallTests: NoDisposalNeeded
{
    public SortingMatchHeapSmallTests(ITestOutputHelper output) : base(output)
    {
    }
    
    
    public struct DataComparer : IEntryComparer<long>
    {
        public int Compare(long x, long y)
        {
            var xd = Data.First(i => i.Key == x);
            var yd = Data.First(i => i.Key ==  y);
            var cmp = string.Compare(xd.Value, yd.Value, StringComparison.Ordinal);
            return cmp == 0 ? x.CompareTo(y) : cmp;
        }

        public long GetEntryId(long x)
        {
            return x;
        }

        public string GetEntryText(long x)
        {
            return Data.First(i => i.Key == x).Value;
        }
    }
    
    [Fact]
    public void DifferentPagesSortProperly()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        
        var heap2 = new SortingMatchHeap<DataComparer, long>(new DataComparer());
        var heap5 = new SortingMatchHeap<DataComparer, long>(new DataComparer());
        bsc.Allocate(2 * sizeof(long), out var mem5);
        bsc.Allocate(5  * sizeof(long), out var mem10);
        heap2.Set(mem5);
        heap5.Set(mem10);
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        
        foreach ((long k, string val) in Data)
        {
            heap2.Add(k);
            heap5.Add(k);
        }

        var fill2 = new long[5];
        heap2.Complete(fill2);
        var fill5 = new long[10];
        heap5.Complete(fill5);
        Assert.Equal(3, heap5.Count);
        Assert.Equal(2,heap2.Count);

        Assert.Equal(fill2.Take(2), fill5.Take(2));

        var actual2 = fill2.Select(l=>Data.Single(x=>x.Key == l)).Take(2).ToArray();
        var expected2 = Data.OrderBy(x => x.Value).ThenBy(x=>x.Key).Take(2).ToArray();
        Assert.Equal(expected2, actual2);
        Assert.Equal(Data.Length,heap5.Count);
        var actual5 = fill5.Select(l=>Data.Single(x=>x.Key == l)).Take(heap5.Count).ToArray();
        var expected5 = Data.OrderBy(x => x.Value).ThenBy(x=>x.Key).Take(heap5.Count).ToArray();
         Assert.Equal(expected5, actual5);
    }
    
    public static (long Key,string Value)[] Data => new (long Key, string Value)[]
    {
        (4456544, "B"), 
        (4456552, "C"), 
        (4456560, "A"), 
        
    };

}
