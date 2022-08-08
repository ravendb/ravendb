using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Corax.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public unsafe class EnumerableWriterScope : IWriterScope
    {
        //todo maciej: this is only temp implementation. Related: https://issues.hibernatingrhinos.com/issue/RavenDB-17243
        private readonly ByteStringContext _allocator;
        
        private readonly List<ByteString> _stringValues;
        private readonly List<long> _longValues;
        private readonly List<double> _doubleValues;
        private readonly List<CoraxSpatialPointEntry> _spatialValues;

        private readonly List<BlittableJsonReaderObject> _blittableJsonReaderObjects;
        private (int Strings, int Longs, int Doubles, int Raws, int Spatials) _count;        

        public EnumerableWriterScope(List<ByteString> stringValues, List<long> longValues, List<double> doubleValues, List<CoraxSpatialPointEntry> spatialValues,
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
            _stringValues.Add(default);
            _longValues.Add(0);
            _doubleValues.Add(float.NaN);
        }

        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            if (_count.Longs != 0 || _count.Doubles != 0)
                throw new InvalidOperationException("Cannot mix tuples writes with straightforward writes");

            // Copy the value to write into memory allocated and controlled by the scope.  
            _allocator.Allocate(value.Length, out var buffer);
            value.CopyTo(buffer.ToSpan());

            _count.Strings++;
            _stringValues.Add(buffer);
            _longValues.Add(0);
            _doubleValues.Add(float.NaN);
        }

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            if (_count.Strings != _count.Longs || _count.Strings != _count.Doubles)
                throw new InvalidOperationException("Cannot write a tuple with a different number of values than the previous tuple.");

            // Copy the value to write into memory allocated and controlled by the scope.  
            _allocator.Allocate(value.Length, out var buffer);
            value.CopyTo(buffer.ToSpan());

            _stringValues.Add(buffer);
            _longValues.Add(longValue);
            _doubleValues.Add(doubleValue);
            _count.Strings++;
            _count.Longs++;
            _count.Doubles++;
        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            _allocator.Allocate(Encoding.UTF8.GetMaxByteCount(value.Length), out var buffer);

            var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
            buffer.Truncate(length);
            _stringValues.Add(buffer);
            _count.Strings++;
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
                entryWriter.Write(field, new ByteStringIterator(_stringValues), CollectionsMarshal.AsSpan(_longValues), CollectionsMarshal.AsSpan(_doubleValues));
            }
            else if (_count is { Raws: > 0, Strings: 0 })
            {
                if (_count.Raws == 1)
                {
                    new BlittableWriterScope(_blittableJsonReaderObjects[0]).Write(field, ref entryWriter);
                }
                else
                {
                    entryWriter.Write(field, new BlittableIterator(_blittableJsonReaderObjects), IndexEntryFieldType.Raw);
                }
            }
            else
            {
                entryWriter.Write(field, new ByteStringIterator(_stringValues));
            }

            var stringSpan = CollectionsMarshal.AsSpan(_stringValues);
            for (int i = 0; i < _stringValues.Count; i++)
            {
                ref var item = ref stringSpan[i];
                if (item.HasValue)
                    _allocator.Release(ref item);
            }
            
            _stringValues.Clear();
            _longValues.Clear();
            _doubleValues.Clear();
            _blittableJsonReaderObjects.Clear();
            _spatialValues.Clear();
        }
        
        private struct ByteStringIterator : IReadOnlySpanIndexer
        {
            private readonly List<ByteString> _values;

            public ByteStringIterator(List<ByteString> values)
            {
                _values = values;
            }

            public int Length => _values.Count;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return !_values[i].HasValue;
            }

            public ReadOnlySpan<byte> this[int i] => IsNull(i) ? ReadOnlySpan<byte>.Empty : _values[i].ToReadOnlySpan();
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

            private ReadOnlySpan<byte> Memory(int id)
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
