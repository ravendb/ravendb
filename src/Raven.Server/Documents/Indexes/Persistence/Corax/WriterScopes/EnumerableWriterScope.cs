using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Sparrow;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class EnumerableWriterScope : IWriterScope
    {
        //todo maciej: this is only temp implementation. Related: https://issues.hibernatingrhinos.com/issue/RavenDB-17243
        private readonly ByteStringContext _allocator;
        private readonly List<Memory<byte>> _stringValues;
        private readonly List<long> _longValues;
        private readonly List<double> _doubleValues;
        private (int Strings, int Longs, int Doubles) _count;


        public EnumerableWriterScope(List<Memory<byte>> stringValues, List<long> longValues, List<double> doubleValues, ByteStringContext allocator)
        {
            _count = (0, 0, 0);
            _doubleValues = doubleValues;
            _longValues = longValues;
            _stringValues = stringValues;
            _allocator = allocator;
        }
        
        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            _count.Strings++;
            _stringValues.Add(new Memory<byte>(value.ToArray()));
        }

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            _stringValues.Add(new Memory<byte>(value.ToArray()));
            _longValues.Add(longValue);
            _doubleValues.Add(doubleValue);
            _count.Strings++;
            _count.Longs++;
            _count.Doubles++;
        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                _stringValues.Add(new Memory<byte>(buffer.ToSpan().ToArray()));
                _count.Strings++;
            }
        }

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            Write(field, value, ref entryWriter);
            _longValues.Add(longValue);
            _doubleValues.Add(doubleValue);
            _count.Strings++;
            _count.Longs++;
            _count.Doubles++;
        }

        public void Finish(int field, ref IndexEntryWriter entryWriter)
        {
            if (_count.Strings == _count.Doubles)
            {
                entryWriter.Write(field, new StringArrayIterator(_stringValues), CollectionsMarshal.AsSpan(_longValues), CollectionsMarshal.AsSpan(_doubleValues));
            }        
            else
            {
                entryWriter.Write(field, new StringArrayIterator(_stringValues));
            }
            _stringValues.Clear();
            _longValues.Clear();
            _doubleValues.Clear();
        }
        private struct StringArrayIterator : IReadOnlySpanEnumerator
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
