using System;
using System.Collections.Generic;
using Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public interface IWriterScope
    {
        public List<int> GetLengthList();
        
        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter);

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter);
        
        public void Write(int field, string value, ref IndexEntryWriter entryWriter);

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter);
    }
}
