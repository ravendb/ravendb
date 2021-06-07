using System;
using System.Collections.Generic;
using Sparrow.Server;
using Voron;
using Voron.Data.Sets;
using Voron.Global;
using Voron.Impl;
using Xunit;

namespace Tryouts
{
    public unsafe class SetLeafPageTests : IDisposable
    {
        private readonly Transaction _tx;
        private readonly StorageEnvironment _env;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseStr;
        private readonly byte* _pagePtr;
        private readonly LowLevelTransaction _llt;

        public SetLeafPageTests()
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
            var leaf = new SetLeafPage(_pagePtr);
            leaf.Init(0);
            var list = new List<long>();
            var buf = new int[] {12, 18};
            var start = 24;
            for (int i = 0; i < size; i++)
            {
                start += buf[i % buf.Length];
                list.Add(start);
                Assert.True(leaf.Add(_llt, start));
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
            var leaf = new SetLeafPage(_pagePtr);
            leaf.Init(0);
            var buf = new int[] {12, 18};
            var start = 24;
            for (int i = 0; i < size; i++)
            {
                start += buf[i % buf.Length];
                Assert.True(leaf.Add(_llt, start));
            }
            
            start = 24;
            for (int i = 0; i < size; i++)
            {
                start += buf[i % buf.Length];
                Assert.True(leaf.Remove(_llt, start));
            }
            Assert.Empty(leaf.GetDebugOutput());
        }

        
        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        [InlineData(257)] // with compressed
        [InlineData(513)] // with compressed x 2 
        [InlineData(4096 + 257)] // with compressed x 16 (so will recompress) 
        public void CanHandleDuplicateValues(int size)
        {
            var leaf = new SetLeafPage(_pagePtr);
            leaf.Init(0);
            var list = new List<long>();
            var buf = new int[] {12, 18};
            var start = 24;
            for (int i = 0; i < size; i++)
            {
                list.Add(start);
                Assert.True(leaf.Add(_llt, start));
                start += buf[i % buf.Length];
            }
            Assert.True(leaf.Add(_llt, 24)); // should be no op
            Assert.Equal(list, leaf.GetDebugOutput());
        }

        public void Dispose()
        {
            _releaseStr.Dispose();
            _tx?.Dispose();
            _env?.Dispose();
        }
    }
}
