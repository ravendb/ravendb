using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    partial class Tree
    {
        public long Increment(string key, long delta)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            return Increment(keySlice, delta);
        }

        public long Increment( string key, long delta, ushort version)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            return Increment(keySlice, delta, version);
        }

        public void Add(string key, Stream value, ushort version)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            Add(keySlice, value, version);
        }

        public void Add(string key, Stream value)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            Add(keySlice, value);
        }

        public void Add(string key, MemoryStream value, ushort? version = null)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            Add(keySlice, value, version);
        }

        public void Add( string key, byte[] value, ushort version)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            var valueSlice = Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable);

            Add(keySlice, valueSlice, version);
        }

        public void Add(string key, byte[] value)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);

            Add(keySlice, value);
        }

        public void Add(string key, string value, ushort? version = null)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            var valueSlice = Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable);

            Add(keySlice, valueSlice, version);
        }

        public unsafe byte* DirectAdd(string key, int len, TreeNodeFlags nodeType = TreeNodeFlags.Data, ushort? version = null)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            return DirectAdd(keySlice, len, nodeType, version);
        }

        public void Delete(string key)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            Delete(keySlice);
        }

        public void Delete(string key, ushort version)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            Delete(keySlice, version);
        }

        public ReadResult Read(string key)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            return Read(keySlice);
        }

        public ushort ReadVersion(string key)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            return ReadVersion(keySlice);
        }


        public void MultiAdd(string key, string value, ushort? version = null)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            var valueSlice = Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable);
            MultiAdd(keySlice, valueSlice, version);
        }

        public void MultiAdd(string key, Slice value, ushort? version = null)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            MultiAdd(keySlice, value, version);
        }

        public void MultiAdd(Slice key, string value, ushort? version = null)
        {
            var valueSlice = Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable);
            MultiAdd(key, valueSlice, version);
        }

        public void MultiDelete(string key, string value, ushort? version = null)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            var valueSlice = Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable);
            MultiDelete(keySlice, valueSlice, version);
        }

        public void MultiDelete(string key, Slice value, ushort? version = null)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            MultiDelete(keySlice, value, version);
        }

        public IIterator MultiRead(string key)
        {
            var keySlice = Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable);
            return MultiRead(keySlice);
        }
    }
}
