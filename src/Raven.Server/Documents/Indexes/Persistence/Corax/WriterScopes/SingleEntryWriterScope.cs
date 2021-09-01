using System;
using System.Collections.Generic;
using System.Text;
using Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class SingleEntryWriterScope : IWriterScope
    {
        private readonly List<int> _lengthList;

        public SingleEntryWriterScope(List<int> lengthList)
        {
            _lengthList = lengthList;
        }
        
        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, value);
        }

        public List<int> GetLengthList() => _lengthList;

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, value, longValue, doubleValue);
        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, Encoding.UTF8.GetBytes(value));
        }

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, Encoding.UTF8.GetBytes(value), longValue, doubleValue);
        }
    }
}
