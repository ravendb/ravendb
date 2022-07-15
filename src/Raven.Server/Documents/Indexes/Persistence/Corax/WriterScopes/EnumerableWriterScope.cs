using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Corax.IndexEntry;
using Corax.Utils;
using K4os.Compression.LZ4.Internal;
using Sparrow;
using Sparrow.Json;
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
        private readonly List<CoraxSpatialPointEntry> _spatialValues;
        private const int MaxSizePerBlittable = (2 << 11);
        private readonly List<BlittableJsonReaderObject> _blittableJsonReaderObjects;
        private (int Strings, int Longs, int Doubles, int Raws, int Spatials) _count;        

        public EnumerableWriterScope(List<Memory<byte>> stringValues, List<long> longValues, List<double> doubleValues, List<CoraxSpatialPointEntry> spatialValues,
            List<BlittableJsonReaderObject> blittableJsonReaderObjects, ByteStringContext allocator)
        {
            _count = (0, 0, 0, 0, 0);
            _doubleValues = doubleValues;
            _longValues = longValues;
            _stringValues = stringValues;
            _spatialValues = spatialValues;
            _blittableJsonReaderObjects = blittableJsonReaderObjects;
            _allocator = allocator;
        }

        public void WriteNull(int field, ref IndexEntryWriter entryWriter)
        {
            // We cannot know if we are writing a tuple or a list. But we know that at finish
            // we will be able to figure out based on the stored counts. Therefore,
            // we will write a null here and then write the real value in the finish method.
            _stringValues.Add(default(Memory<byte>));
            _longValues.Add(0);
            _doubleValues.Add(float.NaN);
        }

        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            if (_count.Longs != 0 || _count.Doubles != 0)
                throw new InvalidOperationException("Cannot mix tuples writes with straightforward writes");

            _count.Strings++;
            _stringValues.Add(new Memory<byte>(value.ToArray()));
            _longValues.Add(0);
            _doubleValues.Add(float.NaN);
        }

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            if (_count.Strings != _count.Longs || _count.Strings != _count.Doubles)
                throw new InvalidOperationException("Cannot write a tuple with a different number of values than the previous tuple.");

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
            _count.Longs++;
            _count.Doubles++;
        }

        public void Write(int field, BlittableJsonReaderObject reader, ref IndexEntryWriter entryWriter)
        {
            _blittableJsonReaderObjects.Add(reader);
            _count.Raws++;
        }

        public void Write(int field, CoraxSpatialPointEntry entry, ref IndexEntryWriter entryWriter)
        {
            _count.Spatials++;
            _spatialValues.Add(entry);
        }

        public void Finish(int field, ref IndexEntryWriter entryWriter)
        {
            if (_count.Spatials != 0)
            {
                if (_count.Spatials == 1)
                    entryWriter.WriteSpatial(field, _spatialValues[0]);
                else
                    entryWriter.WriteSpatial(field, CollectionsMarshal.AsSpan(_spatialValues));

            }
            else if (_count.Raws > 0 && (_count.Longs | _count.Doubles | _count.Strings) != 0)
            {
                // This basically should not happen but I want to make sure on whole SlowTests.
                throw new InvalidDataException($"{nameof(EnumerableWriterScope)}: Some raws were mixed with normal literal.");
            }

            // Even in the case of stored null values, the number of strings and doubles would match. 
            else if (_count.Strings == _count.Doubles && _count.Raws == 0)
            {                
                entryWriter.Write(field, new StringArrayIterator(_stringValues), CollectionsMarshal.AsSpan(_longValues), CollectionsMarshal.AsSpan(_doubleValues));
            }
            else if (_count is { Raws: > 0, Strings: 0 })
            {
                if (_count.Raws == 1)
                {
                    using var blittScope = new BlittableWriterScope(_blittableJsonReaderObjects[0]);
                    blittScope.Write(field, ref entryWriter);
                }
                else
                {
                    using var blittableIterator = new BlittableIterator(_blittableJsonReaderObjects);
                    entryWriter.Write(field, blittableIterator, IndexEntryFieldType.Raw);
                }
            }
            else
            {
                entryWriter.Write(field, new StringArrayIterator(_stringValues));
            }
            
            _stringValues.Clear();
            _longValues.Clear();
            _doubleValues.Clear();
            _blittableJsonReaderObjects.Clear();
            _spatialValues.Clear();
        }

        private struct StringArrayIterator : IReadOnlySpanIndexer
        {
            private readonly List<Memory<byte>> _values;

            public StringArrayIterator(List<Memory<byte>> values)
            {
                _values = values;
            }

            public int Length => _values.Count;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i].Length == 0;
            }

            public ReadOnlySpan<byte> this[int i] => _values[i].Span;
        }

        private struct BlittableIterator : IReadOnlySpanIndexer, IDisposable
        {
            private readonly List<BlittableJsonReaderObject> _values;
            private readonly List<IDisposable> _toDispose;

            public BlittableIterator(List<BlittableJsonReaderObject> values)
            {
                _values = values;
                _toDispose = new();
            }

            public int Length => _values.Count;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return false;
            }

            public ReadOnlySpan<byte> this[int i] => Memory(i);

            private unsafe ReadOnlySpan<byte> Memory(int id)
            {
                var reader = _values[id];
                if (reader.HasParent == false)
                {
                    return new ReadOnlySpan<byte>(reader.BasePointer, reader.Size);
                }

                var clonedBlittable = reader.CloneOnTheSameContext();
                _toDispose.Add(clonedBlittable);
                return new ReadOnlySpan<byte>(clonedBlittable.BasePointer, clonedBlittable.Size);
            }

            public void Dispose()
            {
                foreach (var item in _toDispose)
                {
                    item?.Dispose();
                }
            }
        }
    }
}
