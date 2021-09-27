using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class EnumerableWriterScope : IWriterScope
    {
        //todo maciej: this is only temp implementation. Related: https://issues.hibernatingrhinos.com/issue/RavenDB-17243
        private readonly ByteStringContext _allocator;
        private StringArrayIterator _arrayIterator;
        private readonly List<Memory<byte>> items;
        private readonly List<long?> litems;
        private readonly List<double?> ditems;
        private readonly int _metadataLocation;
        private int _dataLocation;

        public EnumerableWriterScope(int field, ref IndexEntryWriter entryWriter, List<int> stringsLength, ByteStringContext allocator)
        {
            items = new();
            // _allocator = allocator;
            // //entryWriter.PrepareForEnumerable(field, out _metadataLocation);
            // _dataLocation = _metadataLocation;
        }

        public List<int> GetLengthList() => throw new Exception();

        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            // items.WriteEnumerableItem(ref _dataLocation, field, value);
            items.Add(new Memory<byte>(value.ToArray()));
          // _lengthList.Add(value.Length);
        }

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            items.Add(new Memory<byte>(value.ToArray()));
            //entryWriter.WriteEnumerableItem(ref _dataLocation, field, value);
           // _lengthList.Add(value.Length);
        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                //  entryWriter.WriteEnumerableItem(ref _dataLocation, field, buffer.ToSpan());
                items.Add(new Memory<byte>(buffer.ToSpan().ToArray()));
               // _lengthList.Add(length); 
            }
        }

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            //todo maciej: Notice that array could store anything. e.g. [0, "tesT", [DATE]]. So we don't know if it would be only
            //numbers so we store enumerable as string values only.
            Write(field, value, ref entryWriter);
        }

        public void Finish(int field, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, new StringArrayIterator(items));
            //entryWriter.FinishWritingEnumerable(_metadataLocation, _dataLocation, field, _lengthList);
       //     _lengthList.Clear();
        }
        private  struct StringArrayIterator : IReadOnlySpanEnumerator
        {
            private readonly List<Memory<byte>> _values;// = new System.Collections.Generic.List<System.Memory<byte>>();

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
