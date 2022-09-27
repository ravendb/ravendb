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


        public void WriteNull(string path, int field, ref IndexEntryWriter entryWriter)
        {
            if (field == Constants.IndexWriter.DynamicField)
                entryWriter.WriteNullDynamic(path);
            else
                entryWriter.WriteNull(field);
        }
        
        public void Write(string path, int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            if (field == Constants.IndexWriter.DynamicField)
                entryWriter.WriteDynamic(path, value);
            else
                entryWriter.Write(field, value);
        }
        
        public void Write(string path, int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            if (field == Constants.IndexWriter.DynamicField)
                entryWriter.WriteDynamic(path, value, longValue, doubleValue);
            else
                entryWriter.Write(field, value, longValue, doubleValue);
        }

        public void Write(string path, int field, string value, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                if (field == Constants.IndexWriter.DynamicField)
                    entryWriter.WriteDynamic(path, buffer.ToSpan());
                else
                    entryWriter.Write(field, buffer.ToSpan());
            }
        }

        public void Write(string path, int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                if (field == Constants.IndexWriter.DynamicField)
                    entryWriter.WriteDynamic(path, buffer.ToSpan(), longValue, doubleValue);
                else
                    entryWriter.Write(field, buffer.ToSpan(), longValue, doubleValue);
            }
        }

        public void Write(string path, int field, BlittableJsonReaderObject reader, ref IndexEntryWriter entryWriter)
        {
            new BlittableWriterScope(reader).Write(path, field, ref entryWriter);
        }

        public void Write(string path, int field, CoraxSpatialPointEntry entry, ref IndexEntryWriter entryWriter)
        {
            if (field == Constants.IndexWriter.DynamicField)
                entryWriter.WriteSpatialDynamic(path, entry);
            else
                entryWriter.WriteSpatial(field, entry);
        }
    }
}
