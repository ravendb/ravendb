using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class EnumerableWriterScope : IWriterScope
    {
        private readonly ByteStringContext _allocator;
        private readonly List<int> _lengthList;
        private readonly int _metadataLocation;
        private int _dataLocation;

        public EnumerableWriterScope(int field, ref IndexEntryWriter entryWriter, List<int> stringsLength, ByteStringContext allocator)
        {
            _allocator = allocator;
            _lengthList = stringsLength;
            entryWriter.PrepareForEnumerable(field, out _metadataLocation);
            _dataLocation = _metadataLocation;
        }

        public List<int> GetLengthList() => _lengthList;

        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            entryWriter.WriteEnumerableItem(ref _dataLocation, field, value);
            _lengthList.Add(value.Length);
        }

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            entryWriter.WriteEnumerableItem(ref _dataLocation, field, value);
            _lengthList.Add(value.Length);
        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                entryWriter.WriteEnumerableItem(ref _dataLocation, field, buffer.ToSpan());
                _lengthList.Add(length); 
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
            entryWriter.FinishWritingEnumerable(_metadataLocation, _dataLocation, field, _lengthList);
            _lengthList.Clear();
        }
    }
}
