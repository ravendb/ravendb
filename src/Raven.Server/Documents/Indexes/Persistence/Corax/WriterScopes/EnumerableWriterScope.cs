using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Sparrow;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class EnumerableWriterScope : IWriterScope
    {
        //todo maciej: this is only temp implementation. Related: https://issues.hibernatingrhinos.com/issue/RavenDB-17243
        // There is some leftovers but we want to keep them as ideas for further work.
        private readonly ByteStringContext _allocator;
        // private StringArrayIterator _arrayIterator;
        // private readonly List<long?> litems;
        // private readonly List<double?> ditems;
        // private readonly int _metadataLocation;
        // private int _dataLocation;
        private readonly List<Memory<byte>> _items;

        public EnumerableWriterScope(int field, ref IndexEntryWriter entryWriter, List<int> stringsLength, ByteStringContext allocator)
        {
            _items = new();
            _allocator = allocator;
        }

        public List<int> GetLengthList() => throw new Exception();

        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            _items.Add(new Memory<byte>(value.ToArray()));
        }

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            _items.Add(new Memory<byte>(value.ToArray()));

        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                _items.Add(new Memory<byte>(buffer.ToSpan().ToArray()));
            }
        }

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            //todo maciej: impl entryWriter.Write(int field, string value, long? longVal, double? doubleVal);
            Write(field, value, ref entryWriter);
        }

        public void Finish(int field, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, new StringArrayIterator(_items));
        }
        private  struct StringArrayIterator : IReadOnlySpanEnumerator
        {
            private readonly List<Memory<byte>> _values;

            private static string[] Empty = new string[0];

            public StringArrayIterator(List<System.Memory<byte>> values)
            {
                _values = values;
            }
            
            public int Length => _values.Count;

            public ReadOnlySpan<byte> this[int i] => _values[i].Span;
        }
    }
}
