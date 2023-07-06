using System;
using System.Text;
using Corax;
using Corax.Utils;
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

        public void WriteNull(string path, int field, IndexWriter.IndexEntryBuilder entryWriter)
        {
            entryWriter.WriteNull(field, path);
        }
        
        public void Write(string path, int field, ReadOnlySpan<byte> value, IndexWriter.IndexEntryBuilder entryWriter)
        {
            entryWriter.Write(field, path, value);
        }
        
        public void Write(string path, int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, IndexWriter.IndexEntryBuilder entryWriter)
        {
            entryWriter.Write(field, path, value, longValue, doubleValue);
        }

        public void Write(string path, int field, string value, IndexWriter.IndexEntryBuilder entryWriter)
        {
            // cheaper to compute the max size, rather than do the exact encoding
            int maxByteCount = Encoding.UTF8.GetMaxByteCount(value.Length);
            using (_allocator.Allocate(maxByteCount, out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                entryWriter.Write(field, path, buffer.ToSpan());
            }
        }

        public void Write(string path, int field, string value, long longValue, double doubleValue, IndexWriter.IndexEntryBuilder entryWriter)
        {
            int maxByteCount = Encoding.UTF8.GetMaxByteCount(value.Length);
            using (_allocator.Allocate(maxByteCount, out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                entryWriter.Write(field, path, buffer.ToSpan(), longValue, doubleValue);
            }
        }

        public void Write(string path, int field, BlittableJsonReaderObject reader, IndexWriter.IndexEntryBuilder entryWriter)
        {
            new BlittableWriterScope(reader).Write(path, field, entryWriter);
        }

        public void Write(string path, int field, CoraxSpatialPointEntry entry, IndexWriter.IndexEntryBuilder entryWriter)
        {
            entryWriter.WriteSpatial(field, path, entry);
        }
    }
}
