using System;
using Sparrow.Server;

namespace Voron.Util
{
    public unsafe struct ContextBoundNativeList<T> : IDisposable
        where T : unmanaged
    {
        private readonly ByteStringContext _ctx;
        public readonly bool HasContext;
        public NativeList<T> Inner;

        public ContextBoundNativeList(ByteStringContext ctx, int requestedSize = 0)
        {
            _ctx = ctx;
            HasContext = true;
            Inner = new NativeList<T>();
            if (requestedSize > 0)
                Inner.Initialize(ctx, requestedSize);
        }

        public T* RawItems => Inner.RawItems;
        public int Capacity => Inner.Capacity;

        public int Count
        {
            get { return Inner.Count; }
            set { Inner.Count = value; }
        }

        public bool IsValid => RawItems != null;

        public readonly Span<T> ToSpan() => Inner.ToSpan();


        public ref T this[int index]
        {
            get => ref Inner[index];
        }

        public bool TryAdd(in T l)
        {
            return Inner.TryAdd(l);
        }

        public void Add(in T l)
        {
            Inner.EnsureCapacityFor(_ctx, 1);
            Inner.TryAdd(l);
        }

        public void AddUnsafe(in T l)
        {
            Inner.AddUnsafe(l); 
        }

        public ref T AddByRefUnsafe()
        {
            return ref Inner.AddByRefUnsafe();
        }

        public void AddRange(ReadOnlySpan<T> range)
        {
            Inner.EnsureCapacityFor(_ctx, range.Length);

            Inner.AddRangeUnsafe(range);
        }

        public void AddRangeUnsafe(ReadOnlySpan<T> range)
        {
            Inner.AddRangeUnsafe(range);
        }

        public void AddRangeUnsafe(T* range, int count)
        {
            Inner.AddRangeUnsafe(range, count);
        }

        public void Shrink(int newSize)
        {
            Inner.Shrink(newSize); 
        }

        public void Initialize(int count = 1)
        {
            Inner.Initialize(_ctx, count);
        }

        public void Grow(int addition)
        {
            Inner.Grow(_ctx, addition);
        }

        public int CopyTo(Span<T> destination, int startFrom)
        {
            return Inner.CopyTo(destination, startFrom);
        }

        public void CopyTo(Span<T> destination, int startFrom, int count)
        {
            Inner.CopyTo(destination, startFrom, count);
        }

        public readonly void Sort()
        {
            Inner.Sort();
        }

        public void EnsureCapacityFor(int additionalItems)
        {
            Inner.EnsureCapacityFor(_ctx, additionalItems);
        }

        public bool HasCapacityFor(int itemsToAdd)
        {
            return Inner.HasCapacityFor(itemsToAdd);
        }

        public void ResetAndEnsureCapacity(int size)
        {
            Inner.ResetAndEnsureCapacity(_ctx, size);
        }

        public void Dispose()
        {
            Inner.Dispose(_ctx);
        }

        public void Clear()
        {
            Inner.Clear();
        }

        public NativeList<T>.Enumerator GetEnumerator() => new(RawItems, Count);
    }
}
