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

public unsafe class SortingMatchHeapTests: NoDisposalNeeded
{
    public SortingMatchHeapTests(ITestOutputHelper output) : base(output)
    {
    }
    
    
    public struct DataComparer : IEntryComparer<long>
    {
        public int Compare(long x, long y)
        {
            var xd = Data.First(i => i.Key == x);
            var yd = Data.First(i => i.Key ==  y);
            return string.Compare(xd.Value, yd.Value, StringComparison.Ordinal);
        }

        public long GetEntryId(long x)
        {
            return x;
        }
    }
    
    [Fact]
    public void DifferentPagesSortProperly()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        
        var heap5 = new SortingMatchHeap<DataComparer, long>(new DataComparer());
        var heap10 = new SortingMatchHeap<DataComparer, long>(new DataComparer());
        bsc.Allocate(5 * sizeof(long), out var mem5);
        bsc.Allocate(10  * sizeof(long), out var mem10);
        heap5.Set(mem5);
        heap10.Set(mem10);
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        
        foreach ((long k, string id, string val) in Data)
        {
            heap5.Add(k);
        }
        
          
        foreach ((long k, string id, string val) in Data)
        {
            heap10.Add(k);
        }

        var fill5 = new long[5];
        heap5.Complete(fill5);
        Assert.Equal(5,heap5.Count);
        var fill10 = new long[10];
        heap10.Complete(fill10);
        Assert.Equal(10, heap10.Count);
        
        Assert.Equal(fill5, fill10.Take(5));

        var actual5 = fill5.Select(l=>Data.Single(x=>x.Key == l)).ToArray();
        var expected5 = Data.OrderBy(x => x.Value).Take(5).ToArray();
        Assert.Equal(expected5, actual5);
        var actual10 = fill10.Select(l=>Data.Single(x=>x.Key == l)).ToArray();
        var expected10 = Data.OrderBy(x => x.Value).Take(10).ToArray();
         Assert.Equal(expected10, actual10);
    }
    
    public static (long Key, string Id, string Value)[] Data => new (long Key, string Id, string Value)[]
    {
        (4456544, "testitems/2", "2012-09-11T00:00:00.0000000"), (4456548, "testitems/2", "2012-02-27T00:00:00.0000000"),
        (4456552, "testitems/2", "2012-02-09T00:00:00.0000000"), (4456556, "testitems/2", "2012-09-13T00:00:00.0000000"),
        (4456560, "testitems/2", "2012-03-01T00:00:00.0000000"), (4456564, "testitems/3", "2012-04-19T00:00:00.0000000"),
        (4456568, "testitems/3", "2012-10-06T00:00:00.0000000"), (4456572, "testitems/3", "2012-07-05T00:00:00.0000000"),
        (4456576, "testitems/3", "2012-03-26T00:00:00.0000000"), (4456580, "testitems/3", "2012-04-11T00:00:00.0000000"),
        (4456604, "testitems/5", "2012-04-08T00:00:00.0000000"), (4456608, "testitems/5", "2012-03-17T00:00:00.0000000"),
        (4456612, "testitems/5", "2012-06-26T00:00:00.0000000"), (4456616, "testitems/5", "2012-02-26T00:00:00.0000000"),
        (4456620, "testitems/5", "2012-07-04T00:00:00.0000000"), (4456624, "testitems/6", "2012-10-16T00:00:00.0000000"),
        (4456628, "testitems/6", "2012-11-23T00:00:00.0000000"), (4456632, "testitems/6", "2012-11-22T00:00:00.0000000"),
        (4456636, "testitems/6", "2012-07-13T00:00:00.0000000"), (4456640, "testitems/6", "2012-08-14T00:00:00.0000000"),
        (4456684, "testitems/9", "2012-05-13T00:00:00.0000000"), (4456688, "testitems/9", "2012-01-13T00:00:00.0000000"),
        (4456692, "testitems/9", "2012-03-12T00:00:00.0000000"), (4456696, "testitems/9", "2012-11-26T00:00:00.0000000"),
        (4456700, "testitems/9", "2012-02-16T00:00:00.0000000"), (4456704, "testitems/10", " 2012-11-21T00:00:00.0000000"),
        (4456708, "testitems/10", " 2012-09-19T00:00:00.0000000"), (4456712, "testitems/10", " 2012-08-09T00:00:00.0000000"),
        (4456716, "testitems/10", " 2012-05-13T00:00:00.0000000"), (4456720, "testitems/10", " 2012-03-26T00:00:00.0000000"),
        (4481096, "testitems/13", " 2012-06-18T00:00:00.0000000"), (4481100, "testitems/13", " 2012-10-09T00:00:00.0000000"),
        (4481104, "testitems/13", " 2012-01-25T00:00:00.0000000"), (4481108, "testitems/13", " 2012-09-27T00:00:00.0000000"),
        (4481112, "testitems/13", " 2012-07-02T00:00:00.0000000"), (4481116, "testitems/14", " 2012-01-25T00:00:00.0000000"),
        (4481120, "testitems/14", " 2012-07-15T00:00:00.0000000"), (4481124, "testitems/14", " 2012-06-22T00:00:00.0000000"),
        (4481128, "testitems/14", " 2012-02-13T00:00:00.0000000"), (4481132, "testitems/14", " 2012-08-12T00:00:00.0000000"),
        (4481176, "testitems/17", " 2012-07-22T00:00:00.0000000"), (4481180, "testitems/17", " 2012-08-06T00:00:00.0000000"),
        (4481184, "testitems/17", " 2012-10-12T00:00:00.0000000"), (4481188, "testitems/17", " 2012-06-27T00:00:00.0000000"),
        (4481192, "testitems/17", " 2012-01-15T00:00:00.0000000"), (4481196, "testitems/18", " 2012-02-03T00:00:00.0000000"),
        (4481200, "testitems/18", " 2012-04-11T00:00:00.0000000"), (4481204, "testitems/18", " 2012-04-08T00:00:00.0000000"),
        (4481208, "testitems/18", " 2012-11-13T00:00:00.0000000"), (4481212, "testitems/18", " 2012-03-25T00:00:00.0000000"),
        (4481256, "testitems/21", " 2012-08-27T00:00:00.0000000"), (4481260, "testitems/21", " 2012-05-02T00:00:00.0000000"),
        (4481264, "testitems/21", " 2012-08-25T00:00:00.0000000"), (4481268, "testitems/21", " 2012-04-27T00:00:00.0000000"),
        (4481272, "testitems/21", " 2012-07-01T00:00:00.0000000"), (4489292, "testitems/24", " 2012-03-24T00:00:00.0000000"),
        (4489296, "testitems/24", " 2012-06-19T00:00:00.0000000"), (4489300, "testitems/24", " 2012-01-15T00:00:00.0000000"),
        (4489304, "testitems/24", " 2012-08-14T00:00:00.0000000"), (4489308, "testitems/24", " 2012-11-04T00:00:00.0000000"),
        (4489312, "testitems/25", " 2012-09-05T00:00:00.0000000"), (4489316, "testitems/25", " 2012-03-25T00:00:00.0000000"),
        (4489320, "testitems/25", " 2012-06-11T00:00:00.0000000"), (4489324, "testitems/25", " 2012-02-27T00:00:00.0000000"),
        (4489328, "testitems/25", " 2012-01-14T00:00:00.0000000"), (4489372, "testitems/28", " 2012-04-01T00:00:00.0000000"),
        (4489376, "testitems/28", " 2012-04-15T00:00:00.0000000"), (4489380, "testitems/28", " 2012-10-01T00:00:00.0000000"),
        (4489384, "testitems/28", " 2012-05-14T00:00:00.0000000"), (4489388, "testitems/28", " 2012-05-17T00:00:00.0000000"),
        (4489392, "testitems/29", " 2012-10-09T00:00:00.0000000"), (4489396, "testitems/29", " 2012-01-21T00:00:00.0000000"),
        (4489400, "testitems/29", " 2012-04-25T00:00:00.0000000"), (4489404, "testitems/29", " 2012-10-27T00:00:00.0000000"),
        (4489408, "testitems/29", " 2012-07-27T00:00:00.0000000"), (4489452, "testitems/32", " 2012-05-06T00:00:00.0000000"),
        (4489456, "testitems/32", " 2012-02-11T00:00:00.0000000"), (4489460, "testitems/32", " 2012-08-15T00:00:00.0000000"),
        (4489464, "testitems/32", " 2012-03-14T00:00:00.0000000"), (4489468, "testitems/32", " 2012-11-02T00:00:00.0000000"),
        (4489472, "testitems/33", " 2012-11-14T00:00:00.0000000"), (4489476, "testitems/33", " 2012-10-17T00:00:00.0000000"),
        (4489480, "testitems/33", " 2012-02-11T00:00:00.0000000"), (4489484, "testitems/33", " 2012-08-27T00:00:00.0000000"),
        (4489488, "testitems/33", " 2012-01-12T00:00:00.0000000"), (4497508, "testitems/36", " 2012-06-11T00:00:00.0000000"),
        (4497512, "testitems/36", " 2012-11-07T00:00:00.0000000"), (4497516, "testitems/36", " 2012-06-01T00:00:00.0000000"),
        (4497520, "testitems/36", " 2012-01-14T00:00:00.0000000"), (4497524, "testitems/36", " 2012-05-15T00:00:00.0000000"),
        (4497528, "testitems/37", " 2012-01-19T00:00:00.0000000"), (4497532, "testitems/37", " 2012-07-13T00:00:00.0000000"),
        (4497536, "testitems/37", " 2012-11-25T00:00:00.0000000"), (4497540, "testitems/37", " 2012-06-01T00:00:00.0000000"),
        (4497544, "testitems/37", " 2012-07-25T00:00:00.0000000"), (4497584, "testitems/40", " 2012-07-15T00:00:00.0000000"),
        (4497588, "testitems/40", " 2012-09-03T00:00:00.0000000"), (4497592, "testitems/40", " 2012-03-14T00:00:00.0000000"),
        (4497596, "testitems/40", " 2012-10-14T00:00:00.0000000"), (4497600, "testitems/40", " 2012-11-01T00:00:00.0000000"),
        (4497644, "testitems/43", " 2012-03-12T00:00:00.0000000"), (4497648, "testitems/43", " 2012-10-20T00:00:00.0000000"),
        (4497652, "testitems/43", " 2012-07-04T00:00:00.0000000"), (4497656, "testitems/43", " 2012-02-01T00:00:00.0000000"),
        (4497660, "testitems/43", " 2012-04-04T00:00:00.0000000"), (4497664, "testitems/44", " 2012-08-20T00:00:00.0000000"),
        (4497668, "testitems/44", " 2012-06-26T00:00:00.0000000"), (4497672, "testitems/44", " 2012-01-01T00:00:00.0000000"),
        (4497676, "testitems/44", " 2012-07-14T00:00:00.0000000"), (4497680, "testitems/44", " 2012-05-14T00:00:00.0000000"),
        (4505696, "testitems/47", " 2012-04-17T00:00:00.0000000"), (4505700, "testitems/47", " 2012-07-16T00:00:00.0000000"),
        (4505704, "testitems/47", " 2012-05-18T00:00:00.0000000"), (4505708, "testitems/47", " 2012-11-01T00:00:00.0000000"),
        (4505712, "testitems/47", " 2012-09-17T00:00:00.0000000"), (4505716, "testitems/48", " 2012-09-25T00:00:00.0000000"),
        (4505720, "testitems/48", " 2012-04-22T00:00:00.0000000"), (4505724, "testitems/48", " 2012-10-14T00:00:00.0000000"),
        (4505728, "testitems/48", " 2012-05-15T00:00:00.0000000"), (4505732, "testitems/48", " 2012-11-27T00:00:00.0000000"),
    };

}
