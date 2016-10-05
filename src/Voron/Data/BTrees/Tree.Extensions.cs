using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.Fixed;

namespace Voron.Data.BTrees
{
    partial class Tree
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        public long Increment(string key, long delta)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return Increment(keySlice, delta);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public long Increment( string key, long delta, ushort version)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return Increment(keySlice, delta, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool AddMax(string key, long value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return AddMax(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, Stream value, ushort version)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, Stream value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, MemoryStream value, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, byte[] value, ushort version)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                Add(keySlice, valueSlice, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, byte[] value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, string value, ushort? version = null)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                Add(keySlice, valueSlice, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public unsafe byte* DirectAdd(string key, int len, TreeNodeFlags nodeType = TreeNodeFlags.Data, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return DirectAdd(keySlice, len, nodeType, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Delete(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                Delete(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Delete(string key, ushort version)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                Delete(keySlice, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public ReadResult Read(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return Read(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public ushort ReadVersion(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return ReadVersion(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(string key, string value, ushort? version = null)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                MultiAdd(keySlice, valueSlice, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(string key, Slice value, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                MultiAdd(keySlice, value, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(Slice key, string value, ushort? version = null)
        {
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                MultiAdd(key, valueSlice, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(string key, string value, ushort? version = null)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                MultiDelete(keySlice, valueSlice, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(string key, Slice value, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                MultiDelete(keySlice, value, version);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public IIterator MultiRead(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return MultiRead(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public long DeleteFixedTreeFor(string key, byte valSize = 0)
        {
            Slice keySlice;
            Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice);
            return DeleteFixedTreeFor(keySlice, valSize);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public FixedSizeTree FixedTreeFor(string key, byte valSize = 0)
        {
            Slice keySlice; // we explicitly don't dispose it here
            Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice);
            return FixedTreeFor(keySlice, valSize);
        }
    }
}
