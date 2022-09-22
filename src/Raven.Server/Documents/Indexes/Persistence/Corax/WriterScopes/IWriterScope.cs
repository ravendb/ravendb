using System;
using System.Collections.Generic;
using Corax;
using Corax.Utils;
using Raven.Server.Documents.Indexes.Spatial;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public interface IWriterScope
    {
        public void WriteNull(string path, int field, ref IndexEntryWriter entryWriter);
        public void Write(string path, int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter);

        public void Write(string path, int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter);
        
        public void Write(string path, int field, string value, ref IndexEntryWriter entryWriter);
        
        public void Write(string path, int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter);
        
        public void Write(string path, int field, BlittableJsonReaderObject reader, ref IndexEntryWriter entryWriter);

        public void Write(string path, int field, CoraxSpatialPointEntry entry, ref IndexEntryWriter entryWriter);
    }
}
