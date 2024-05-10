using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using Voron.Data.Fixed;
using Voron.Data.Lookups;

namespace Voron.Data.BTrees
{
    unsafe partial class Tree
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        public long Increment(string key, long delta)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                return Increment(keySlice, delta);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, Stream value)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                Add(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, byte[] value)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                Add(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, string value)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out Slice valueSlice))
            {
                Add(keySlice, valueSlice);
            }
        }

        public DirectAddScope DirectAdd(string key, int len, out byte* ptr)
        {
            return DirectAdd(key, len, TreeNodeFlags.Data, out ptr);
        }


        [MethodImpl(MethodImplOptions.NoInlining)]
        public DirectAddScope DirectAdd(string key, int len, TreeNodeFlags nodeType, out byte* ptr)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                return DirectAdd(keySlice, len, nodeType, out ptr);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Delete(string key)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                Delete(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public ReadResult Read(string key)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                return Read(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool Exists(string key)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                return Exists(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(string key, string value)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out Slice valueSlice))
            {
                MultiAdd(keySlice, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(string key, Slice value)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                MultiAdd(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(Slice key, string value)
        {
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out Slice valueSlice))
            {
                MultiAdd(key, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(string key, string value)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out Slice valueSlice))
            {
                MultiDelete(keySlice, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(Slice key, string value)
        {
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out Slice valueSlice))
            {
                MultiDelete(key, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(string key, Slice value)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                MultiDelete(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public IIterator MultiRead(string key)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                return MultiRead(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public long DeleteFixedTreeFor(string key, byte valSize = 0)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                return DeleteFixedTreeFor(keySlice, valSize);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public FixedSizeTree FixedTreeFor(string key, byte valSize = 0)
        {
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
            {
                return FixedTreeFor(keySlice, valSize);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public FixedSizeTree<TVal> FixedSizeTree<TVal>(Tree fieldsTree, Slice fieldName, byte valSize = 0)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            if (typeof(TVal) == typeof(long))
            {
                return (FixedSizeTree<TVal>)(object)fieldsTree.FixedTreeFor(fieldName, valSize);
            }

            if (typeof(TVal) == typeof(double))
            {
                return (FixedSizeTree<TVal>)(object)fieldsTree.FixedTreeForDouble(fieldName, valSize);
            }

            throw new NotSupportedException();
        }

        public long GetLookupRootPage(Slice name)
        {
            if (TryRead(name, out var reader) == false)
                return -1;
            
            var header = (LookupState*)reader.Base;
            if (header->RootObjectType != RootObjectType.Lookup)
                return -1;
            
            return header->RootPage;
        }

        public Dictionary<long, string> GetFieldsRootPages()
        {
            var dic = new Dictionary<long, string>();
            using var it = Iterate(prefetch: false);
            if (it.Seek(Slices.BeforeAllKeys) == false)
                return dic;
            do
            {
                if(it.GetCurrentDataSize() != sizeof(LookupState))
                    continue;
                
                var header = (LookupState*)it.CreateReaderForCurrent().Base;
                if(header->RootObjectType != RootObjectType.Lookup)
                    continue;

                dic[header->RootPage] = it.CurrentKey.ToString();

            } while (it.MoveNext());

            return dic;
        }
    }
}
