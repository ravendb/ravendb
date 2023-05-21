using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Sets
{
    public unsafe class PostingListLeafPageTests : NoDisposalNeeded
    {
        private readonly Transaction _tx;
        private readonly StorageEnvironment _env;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseStr;
        private readonly byte* _pagePtr;
        private readonly LowLevelTransaction _llt;

        public PostingListLeafPageTests(ITestOutputHelper output) : base(output)
        {
            _env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly());
            _tx = _env.WriteTransaction();
            _llt = _tx.LowLevelTransaction;
            _releaseStr = _tx.Allocator.Allocate(Constants.Storage.PageSize, out var bs);
            _pagePtr = bs.Ptr;
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        [InlineData(257)] // with compressed
        [InlineData(513)] // with compressed x 2 
        [InlineData(4096 + 257)] // with compressed x 16 (so will recompress) 
        public void CanAddAndRead(int size)
        {
            var leaf = new PostingListLeafPage(new Page(_pagePtr));
            PostingListLeafPage.InitLeaf(leaf.Header);
            var list = new List<long>();
            var buf = new int[] {12, 18};
            var start = 24;
            for (int i = 0; i < size; i++)
            {
                start += buf[i % buf.Length];
                list.Add(start);
            }

            Span<long> span = list.ToArray();
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var tempList = new NativeIntegersList(bsc);
            fixed (long* p = span)
            {
                var len = span.Length;
                var rp = p;
                long* rr = null;
                var zero = 0;
                var extras = leaf.Update(_llt,tempList, ref rp, ref len, ref rr, ref zero, long.MaxValue);
                Assert.Equal(0, extras);
                Assert.Equal(0, len);
                Assert.Equal((long)(p+ span.Length), (long)rp);
            }

            Assert.Equal(list, leaf.GetDebugOutput());
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        [InlineData(257)] // with compressed
        [InlineData(513)] // with compressed x 2 
        [InlineData(4096 + 257)] // with compressed x 16 (so will recompress) 
        public void CanAddAndRemove(int size)
        {
            var leaf = new PostingListLeafPage(new Page(_pagePtr));
            PostingListLeafPage.InitLeaf(leaf.Header);
            var buf = new int[] {12, 18};
            var start = 24;
            var list = new long[size];
            for (int i = 0; i < size; i++)
            {
                start += buf[i % buf.Length];
                list[i] = start;
            }
            Span<long> additions = list;
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var tempList = new NativeIntegersList(bsc);
            fixed (long* p = additions)
            {
                var pp = p;
                var pl = additions.Length;
                long* none = null;
                int zero = 0;
                var extras = leaf.Update(_llt,tempList, ref pp, ref pl, ref none, ref zero, long.MaxValue);
                Assert.Equal(0, extras);
                Assert.Equal(0, pl);
            }
            
            Assert.NotEmpty(leaf.GetDebugOutput());
            
            Span<long> reomvals = list; // now remove
            fixed (long* p = reomvals)
            {
                var pp = p;
                var pl = additions.Length;
                long* none = null;
                int zero = 0;
                var extras = leaf.Update(_llt, tempList, ref none, ref zero, ref pp, ref pl, long.MaxValue);
                Assert.Equal(0, extras);
                Assert.Equal(0, pl);
                Assert.Empty(leaf.GetDebugOutput());
            }
        }

        
        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        [InlineData(257)] // with compressed
        [InlineData(513)] // with compressed x 2 
        [InlineData(4096 + 257)] // with compressed x 16 (so will recompress) 
        public void CanHandleDuplicateValues(int size)
        {
            var leaf = new PostingListLeafPage(new Page(_pagePtr));
            PostingListLeafPage.InitLeaf(leaf.Header);
            var list = new List<long>();
            var buf = new int[] {12, 18};
            var start = 24;
            for (int i = 0; i < size; i++)
            {
                list.Add(start);
                start += buf[i % buf.Length];
            }
            Span<long> additions = list.ToArray();

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var tempList = new NativeIntegersList(bsc);

            fixed (long* p = additions)
            {
                var pp = p;
                var pl = additions.Length;
                long* none = null;
                int zero = 0;
                var extras = leaf.Update(_llt, tempList, ref pp, ref pl, ref none, ref zero, long.MaxValue);
                Assert.Equal(0, extras);
                Assert.Equal(0, pl);
            }
            additions = new long[] { 24 };
            
            fixed (long* p = additions)
            {
                var pp = p;
                var pl = additions.Length;
                long* none = null;
                int zero = 0;
                var extras = leaf.Update(_llt, tempList,ref pp, ref pl, ref none, ref zero, long.MaxValue);
                Assert.Equal(0,extras);
                Assert.Equal(0, pl);
            }

            Assert.Equal(list, leaf.GetDebugOutput());
        }

        public override void Dispose()
        {
            try
            {
                _releaseStr.Dispose();
                _tx?.Dispose();
                _env?.Dispose();
            }
            finally
            {
                base.Dispose();
            }
        }
    }
}
