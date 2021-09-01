using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Sparrow.Threading;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class EnumerableWriterScope : IWriterScope
    {
        private readonly List<int> _lengthList;
        private readonly int _metadataLocation;
        private int _dataLocation;

        public EnumerableWriterScope(int field, ref IndexEntryWriter entryWriter, List<int> stringsLength)
        {
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
            var valueInBytes = Encoding.UTF8.GetBytes(value);
            entryWriter.WriteEnumerableItem(ref _dataLocation, field, valueInBytes);
            _lengthList.Add(valueInBytes.Length);       
        }

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            var valueInBytes = Encoding.UTF8.GetBytes(value);
            entryWriter.WriteEnumerableItem(ref _dataLocation, field, valueInBytes);
            _lengthList.Add(valueInBytes.Length);

        }

        public void Finish(int field, ref IndexEntryWriter entryWriter)
        {
            entryWriter.FinishWritingEnumerable(_metadataLocation, _dataLocation, field, _lengthList);
            _lengthList.Clear();
        }
    }
}
