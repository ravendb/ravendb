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
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return Increment(keySlice, delta);
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
        public void Add(string key, byte[] value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, string value)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
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
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return DirectAdd(keySlice, len, nodeType, out ptr);
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
        public ReadResult Read(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return Read(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool Exists(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                return Exists(keySlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(string key, string value)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                MultiAdd(keySlice, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(string key, Slice value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                MultiAdd(keySlice, value);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiAdd(Slice key, string value)
        {
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                MultiAdd(key, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(string key, string value)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                MultiDelete(keySlice, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(Slice key, string value)
        {
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, value, ByteStringType.Immutable, out valueSlice))
            {
                MultiDelete(key, valueSlice);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MultiDelete(string key, Slice value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out keySlice))
            {
                MultiDelete(keySlice, value);
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

        public long GetLookupRootPage(Slice name)
        {
            var result = Read(name);
            if (result == null)
                return -1;
            var header = (LookupState*)result.Reader.Base;
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
