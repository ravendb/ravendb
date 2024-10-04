using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public sealed unsafe class BlittableJsonReaderArray : BlittableJsonReaderBase, IEnumerable<object>, IDisposable
    {
        private bool _disposeParent;
        private readonly int _count;
        private readonly byte* _metadataPtr;
        private readonly byte* _dataStart;
        private readonly long _currentOffsetSize;
        private Dictionary<int, (object, BlittableJsonToken)> _cache;

        public DynamicJsonArray Modifications;

        public BlittableJsonReaderObject Parent => _parent;

        internal void ArrayIsRoot() => _disposeParent = true;

        public BlittableJsonReaderArray(int pos, BlittableJsonReaderObject parent, BlittableJsonToken type)
            : base(parent._context)
        {
            _parent = parent;

            _count = parent.ReadVariableSizeInt(pos, out var arraySizeOffset);

            _dataStart = parent.BasePointer + pos;
            _metadataPtr = _dataStart + arraySizeOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
        }

        public byte* DataStart => _dataStart;
        public int Length => _count;

        public override string ToString()
        {
            if (_parent._mem == null)
                return "Disposed";

            AssertContextNotDisposed();

            using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
            {
                var tw = new AsyncBlittableJsonTextWriter(_context, memoryStream);
                try
                {
                    tw.WriteValue(BlittableJsonToken.StartArray, this);
                    tw.FlushAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
                    memoryStream.Position = 0;

                    return new StreamReader(memoryStream).ReadToEnd();
                }
                finally
                {
                    tw.DisposeAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
        }

        public void BlittableValidation()
        {
            AssertContextNotDisposed();
            _parent?.BlittableValidation();
        }

        //Todo Fixing the clone implementation to support this situation or throw clear error
        public BlittableJsonReaderArray Clone(JsonOperationContext context, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            AssertContextNotDisposed();
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                builder.Reset(usageMode);
                builder.StartArrayDocument();
                builder.StartWriteArray();
                using (var itr = new BlittableJsonArrayEnumerator(this))
                {
                    while (itr.MoveNext())
                    {
                        switch (itr.Current)
                        {
                            case BlittableJsonReaderObject item:
                                var clone = item.CloneOnTheSameContext();
                                builder.WriteEmbeddedBlittableDocument(clone.BasePointer, clone.Size);
                                break;

                            case LazyStringValue item:
                                builder.WriteValue(item);
                                break;

                            case long item:
                                builder.WriteValue(item);
                                break;

                            case LazyNumberValue item:
                                builder.WriteValue(item);
                                break;

                            case LazyCompressedStringValue item:
                                builder.WriteValue(item);
                                break;

                            default:
                                throw new InvalidDataException($"Actual value type is {itr.Current.GetType()}. Should be known serialized type and should not happen. ");
                        }
                    }
                }
                builder.WriteArrayEnd();
                builder.FinalizeDocument();

                return builder.CreateArrayReader();
            }
        }

        public void Dispose()
        {
            AssertContextNotDisposed();

            // this is required only in cases that we get a BlittableJsonReaderArray, which is an only child of an BlittableJsonReaderObject and we lose track of it's parent,
            // like in BlittableJsonDocumentBuilder.CreateArrayReader.
            if (_disposeParent)
                _parent?.Dispose();
        }

        public BlittableJsonToken GetArrayType()
        {
            AssertContextNotDisposed();
            var blittableJsonToken = (BlittableJsonToken)(*(_metadataPtr + _currentOffsetSize)) & TypesMask;
            Debug.Assert(blittableJsonToken != 0);
            return blittableJsonToken;
        }

        public object this[int index] => GetValueTokenTupleByIndex(index).Item1;

        public int BinarySearch(string key, StringComparison comparison)
        {
            AssertContextNotDisposed();
            int min = 0;
            int max = Length - 1;

            while (min <= max)
            {
                int mid = (min + max) >> 1;
                var current = GetStringByIndex(mid);
                var result = string.Compare(key, current, comparison);
                if (result == 0)
                {
                    return mid;
                }
                else if (result < 0)
                {
                    max = mid - 1;
                }
                else
                {
                    min = mid + 1;
                }
            }
            return ~(((min + max) >> 1) + 1);
        }

        public T GetByIndex<T>(int index)
        {
            AssertContextNotDisposed();
            var obj = GetValueTokenTupleByIndex(index).Item1;
            BlittableJsonReaderObject.ConvertType(obj, out T result);
            return result;
        }

        public string GetStringByIndex(int index)
        {
            AssertContextNotDisposed();
            var obj = GetValueTokenTupleByIndex(index).Item1;
            if (obj == null)
                return null;

            if (obj is LazyStringValue lazyStringValue)
                return (string)lazyStringValue;
            if (obj is LazyCompressedStringValue lazyCompressedStringValue)
                return lazyCompressedStringValue;
            BlittableJsonReaderObject.ConvertType(obj, out string result);
            return result;
        }

        public void AddItemsToStream<T>(ManualBlittableJsonDocumentBuilder<T> writer) where T : struct, IUnmanagedWriteBuffer
        {
            AssertContextNotDisposed();
            for (var i = 0; i < _count; i++)
            {
                var (value, token) = GetValueTokenTupleByIndex(i);
                writer.WriteValue(ProcessTokenTypeFlags(token), value);
            }
        }

        public (object, BlittableJsonToken) GetValueTokenTupleByIndex(int index)
        {
            AssertContextNotDisposed();

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (NoCache == false && _cache != null && _cache.TryGetValue(index, out (object, BlittableJsonToken) result))
                return result;

            if (index >= _count || index < 0)
                throw new IndexOutOfRangeException($"Cannot access index {index} when our size is {_count}");

            var itemMetadataStartPtr = _metadataPtr + index * (_currentOffsetSize + 1);
            var offset = ReadNumber(itemMetadataStartPtr, _currentOffsetSize);
            var token = *(itemMetadataStartPtr + _currentOffsetSize);
            result = (
                _parent.GetObject((BlittableJsonToken)token,(int)(_dataStart - _parent.BasePointer - offset), out bool isBlittableJsonReader), 
                (BlittableJsonToken)token & TypesMask
            );

            if (isBlittableJsonReader)
            {
                ((BlittableJsonReaderBase)result.Item1).NoCache = NoCache;
                if (NoCache == false)
                {
                    _cache ??= new Dictionary<int, (object, BlittableJsonToken)>();
                    _cache[index] = result;
                }
            }
            return result;
        }

        public BlittableJsonArrayEnumerator Items
        {
            get
            {
                AssertContextNotDisposed();
                return new BlittableJsonArrayEnumerator(this);
            }
        }

        public struct BlittableJsonArrayEnumerator : IEnumerator<object>, IEnumerable<object>
        {
            private readonly BlittableJsonReaderArray _reader;
            private int _counter;

            public BlittableJsonArrayEnumerator(BlittableJsonReaderArray reader)
            {
                _reader = reader;
                _counter = -1;
            }

            public bool MoveNext()
            {
                _counter++;
                return _counter < _reader._count;
            }

            public void Reset()
            {
                _counter = -1;
            }

            public object Current { get { return _reader[_counter]; } }

            object IEnumerator.Current => Current;

            public void Dispose() {}

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IEnumerator<object> GetEnumerator()
            {
                return Enumerate();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            IEnumerator IEnumerable.GetEnumerator()
            {
                return Enumerate();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public BlittableJsonArrayEnumerator Enumerate()
            {
                return this;
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator<object> IEnumerable<object>.GetEnumerator()
        {
            return GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        BlittableJsonArrayEnumerator GetEnumerator()
        {
            AssertContextNotDisposed();

            return new BlittableJsonArrayEnumerator(this);
        }

        public override bool Equals(object obj)
        {
            AssertContextNotDisposed();

            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            if (obj is BlittableJsonReaderArray array)
                return Equals(array);

            return false;
        }

        public bool Equals(BlittableJsonReaderArray other)
        {
            AssertContextNotDisposed();

            if (_count != other._count)
                return false;

            for (int i = 0; i < _count; i++)
            {
                var x = this[i];
                var y = other[i];

                if (x == null && y == null)
                    continue;

                if ((x?.Equals(y) ?? false) == false)
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            AssertContextNotDisposed();
            return _count;
        }

        public void EnsureArrayModifiable()
        {
            Modifications ??= new DynamicJsonArray(this);
            Modifications.SkipOriginalArray = true;
        }
    }
}
