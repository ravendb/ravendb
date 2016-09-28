using System.IO;

namespace Voron.Data.BTrees
{
    partial class Tree
    {
        public long Increment(string key, long delta)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                return Increment(keySlice, delta);
            }
        }

        public long Increment( string key, long delta, ushort version)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                return Increment(keySlice, delta, version);
            }
        }

        public bool AddMax(string key, long value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                return AddMax(keySlice, value);
            }
        }

        public void Add(string key, Stream value, ushort version)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value, version);
            }
        }

        public void Add(string key, Stream value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value);
            }
        }

        public void Add(string key, MemoryStream value, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value, version);
            }
        }

        public void Add(string key, byte[] value, ushort version)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable, out valueSlice))
            {
                Add(keySlice, valueSlice, version);
            }
        }

        public void Add(string key, byte[] value)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                Add(keySlice, value);
            }
        }

        public void Add(string key, string value, ushort? version = null)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable, out valueSlice))
            {
                Add(keySlice, valueSlice, version);
            }
        }

        public unsafe byte* DirectAdd(string key, int len, TreeNodeFlags nodeType = TreeNodeFlags.Data, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                return DirectAdd(keySlice, len, nodeType, version);
            }
        }

        public void Delete(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                Delete(keySlice);
            }
        }

        public void Delete(string key, ushort version)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                Delete(keySlice, version);
            }
        }

        public ReadResult Read(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                return Read(keySlice);
            }
        }

        public ushort ReadVersion(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                return ReadVersion(keySlice);
            }
        }


        public void MultiAdd(string key, string value, ushort? version = null)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable, out valueSlice))
            {
                MultiAdd(keySlice, valueSlice, version);
            }
        }

        public void MultiAdd(string key, Slice value, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                MultiAdd(keySlice, value, version);
            }
        }

        public void MultiAdd(Slice key, string value, ushort? version = null)
        {
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable, out valueSlice))
            {
                MultiAdd(key, valueSlice, version);
            }
        }

        public void MultiDelete(string key, string value, ushort? version = null)
        {
            Slice keySlice;
            Slice valueSlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            using (Slice.From(_llt.Allocator, value, Sparrow.ByteStringType.Immutable, out valueSlice))
            {
                MultiDelete(keySlice, valueSlice, version);
            }
        }

        public void MultiDelete(string key, Slice value, ushort? version = null)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                MultiDelete(keySlice, value, version);
            }
        }

        public IIterator MultiRead(string key)
        {
            Slice keySlice;
            using (Slice.From(_llt.Allocator, key, Sparrow.ByteStringType.Immutable, out keySlice))
            {
                return MultiRead(keySlice);
            }
        }
    }
}
