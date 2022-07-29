using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Utils;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents.Indexes.Spatial;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class SingleEntryWriterScope : IWriterScope
    {
        private readonly ByteStringContext _allocator;

        public SingleEntryWriterScope(ByteStringContext allocator)
        {
            _allocator = allocator;
        }


        public void WriteNull(int field, ref IndexEntryWriter entryWriter)
        {
            entryWriter.WriteNull(field);
        }
        
        public void WriteEmpty(int field, ref IndexEntryWriter entryWriter)
        {
            entryWriter.WriteEmpty(field);
        }
        
        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, value);
        }
        
        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, value, longValue, doubleValue);
        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                entryWriter.Write(field, buffer.ToSpan());
            }
        }

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                entryWriter.Write(field, buffer.ToSpan(), longValue, doubleValue);
            }
        }

        public void Write(int field, BlittableJsonReaderObject reader, ref IndexEntryWriter entryWriter)
        {
            using var scope = new BlittableWriterScope(reader);
            scope.Write(field, ref entryWriter);
        }

        public void Write(int field, CoraxSpatialPointEntry entry, ref IndexEntryWriter entryWriter)
        {
            entryWriter.WriteSpatial(field, entry);
        }
    }
}
