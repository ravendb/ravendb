using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Org.BouncyCastle.Crypto;
using Raven.Client.Documents.Linq;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
   
    public class ObjectPoolTests : NoDisposalNeeded
    {
        public class MyPooleableType : IDisposable
        {
            public int Index;
            public bool IsDisposed;

            private static int _counter;
            
            public MyPooleableType()
            {
                Index = Interlocked.Increment(ref _counter);
            }
            
            public void Dispose()
            {
                IsDisposed = true;
            }
        }

        [Fact]
        public void AllocateAndFree()
        {
            var pool = new ObjectPool<MyPooleableType>(() => new MyPooleableType());
            var o1 = pool.Allocate();
            Assert.NotNull(o1);
            pool.Free(o1);
            var o2 = pool.Allocate();
            Assert.Equal(o1, o2);
        }

        [Fact]
        public void AllocateInContextAndFree()
        {
            var pool = new ObjectPool<MyPooleableType>(() => new MyPooleableType());

            var context = pool.AllocateInContext();
            var o1 = context.Value;
            Assert.NotNull(o1);
            context.Dispose();

            context = pool.AllocateInContext();
            var o2 = context.Value;
            Assert.Equal(o1, o2);
        }

        [Fact]
        public void AllocateMultipleAndFree()
        {
            var pool = new ObjectPool<MyPooleableType>(() => new MyPooleableType(), 10);

            var first = new MyPooleableType[10];
            for (int i = 0; i < first.Length; i++)
            {
                first[i] = pool.Allocate();
                Assert.NotNull(first[i]);
            }

            for (int i = 0; i < first.Length; i++)
            {
                pool.Free(first[i]);
            }

            var second = new MyPooleableType[first.Length];
            for (int i = 0; i < second.Length; i++)
            {
                second[i] = pool.Allocate();
                Assert.NotNull(second[i]);
            }

            foreach (var item in first)
            {
                Assert.Contains(item, second);
            }
        }

        [Fact]
        public void AllocateThreadAwareMultipleAndFree()
        {
            var pool = new ObjectPool<MyPooleableType, NoResetSupport<MyPooleableType>, ThreadAwareBehavior>(() => new MyPooleableType(), 10);

            var first = new MyPooleableType[10];
            for (int i = 0; i < first.Length; i++)
            {
                first[i] = pool.Allocate();
                Assert.NotNull(first[i]);
            }

            for (int i = 0; i < first.Length; i++)
            {
                pool.Free(first[i]);
            }

            var second = new MyPooleableType[first.Length];
            for (int i = 0; i < second.Length; i++)
            {
                second[i] = pool.Allocate();
                Assert.NotNull(second[i]);
            }

            foreach (var item in first)
            {
                Assert.Contains(item, second);
            }
        }

        [Fact]
        public void PartialClear()
        {
            var pool = new ObjectPool<MyPooleableType, NoResetSupport<MyPooleableType>, ThreadAwareBehavior>(() => new MyPooleableType(), 10);

            var first = new MyPooleableType[5];
            for (int i = 0; i < first.Length; i++)
            {
                first[i] = pool.Allocate();
                Assert.NotNull(first[i]);
            }

            for (int i = 0; i < first.Length; i++)
            {
                pool.Free(first[i]);
            }

            pool.Clear();

            Assert.Contains(pool.Allocate(), first);
            Assert.Contains(pool.Allocate(), first);
            Assert.Contains(pool.Allocate(), first);
            Assert.Contains(pool.Allocate(), first);
            Assert.DoesNotContain(pool.Allocate(), first);
        }

        [Fact]
        public void Clear()
        {
            var pool = new ObjectPool<MyPooleableType, NoResetSupport<MyPooleableType>, ThreadAwareBehavior>(() => new MyPooleableType(), 10);

            var first = new MyPooleableType[5];
            for (int i = 0; i < first.Length; i++)
            {
                first[i] = pool.Allocate();
                Assert.NotNull(first[i]);
            }

            for (int i = 0; i < first.Length; i++)
            {
                pool.Free(first[i]);
            }

            pool.Clear(false);

            Assert.DoesNotContain(pool.Allocate(), first);
            Assert.DoesNotContain(pool.Allocate(), first);
            Assert.DoesNotContain(pool.Allocate(), first);
            Assert.DoesNotContain(pool.Allocate(), first);
            Assert.DoesNotContain(pool.Allocate(), first);
        }

        private struct EvictionPolicy : IEvictionStrategy<MyPooleableType>
        {
            private readonly long _now;
            private readonly long _idle;

            public EvictionPolicy(long now, long idle)
            {
                this._now = now;
                this._idle = idle;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool CanEvict(MyPooleableType item)
            {
                var timeInPool = _now - item.Index;
                return timeInPool >= _idle;
            }
        }

        [Fact]
        public void ClearWithEviction()
        {
            var pool = new ObjectPool<MyPooleableType, NoResetSupport<MyPooleableType>, ThreadAwareBehavior>(() => new MyPooleableType(), 10);

            var first = new MyPooleableType[10];
            for (int i = 0; i < first.Length; i++)
            {
                first[i] = pool.Allocate();
                Assert.NotNull(first[i]);
            }

            for (int i = 0; i < first.Length; i++)
            {
                first[i].Index = first.Length - i;
                pool.Free(first[i]);
            }

            // We kill the lowest 5 elements from the cache (the highest distance). 
            pool.Clear(new EvictionPolicy(10, 5));

            // These are in the thread aware cache
            Assert.Contains(pool.Allocate(), first);
            Assert.Contains(pool.Allocate(), first);
            Assert.Contains(pool.Allocate(), first);
            Assert.Contains(pool.Allocate(), first);
            
            // This on is in the partial cache
            Assert.Contains(pool.Allocate(), first);
            
            // This bucket should be dead already. 
            Assert.DoesNotContain(pool.Allocate(), first);

            foreach (var item in first)
            {
                if (item.Index <= 5)
                    Assert.True(item.IsDisposed);
                else
                    Assert.False(item.IsDisposed);
            }                                   
        }
    }
}
