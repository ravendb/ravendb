using System;
using System.Collections;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class ReduceKeyProcessor
    {
        private readonly UnmanagedBuffersPoolWithLowMemoryHandling _buffersPool;
        private Mode _mode;
        private AllocatedMemoryData _buffer;
        private int _bufferPos;
        private ulong _singleValueHash;
        private readonly int _numberOfReduceFields;
        private int _processedFields;
        private bool _hadAnyNotNullValue;

        public ReduceKeyProcessor(int numberOfReduceFields, UnmanagedBuffersPoolWithLowMemoryHandling buffersPool)
        {
            _numberOfReduceFields = numberOfReduceFields;
            _buffersPool = buffersPool;
            _bufferPos = 0;

            if (numberOfReduceFields == 1)
            {
                _mode = Mode.SingleValue;
            }
            else
            {
                // numberOfReduceFields could be zero when we have 'group bankTotal by 1'
                _mode = Mode.MultipleValues;
            }
        }

        public void SetMode(Mode mode)
        {
            _mode = mode;
        }

        public int ProcessedFields => _processedFields;

        public void Reset()
        {
            _bufferPos = 0;
            _processedFields = 0;
            _hadAnyNotNullValue = false;
        }

        public ulong Hash
        {
            get
            {
                if (_processedFields != _numberOfReduceFields)
                    ThrowInvalidNumberOfFields();

                if (_hadAnyNotNullValue == false)
                    return 0;

                switch (_mode)
                {
                    case Mode.SingleValue:
                        return _singleValueHash;
                    case Mode.MultipleValues:
                        if (_buffer == null)
                            return 0; // this can happen if we have _no_ values (group by "constant")
                        return Hashing.XXHash64.CalculateInline(_buffer.Address, (ulong)_bufferPos);
                    default:
                        ThrowUnknownReduceValueMode();
                        return 0;// never hit
                }
            }
        }

        private void ThrowUnknownReduceValueMode()
        {
            throw new NotSupportedException($"Unknown reduce value processing mode: {_mode}");
        }

        private void ThrowInvalidNumberOfFields()
        {
            throw new InvalidOperationException($"It processed {_processedFields} while expected to get {_numberOfReduceFields}");
        }

        public void Process(ByteStringContext context,object value, bool internalCall = false)
        {
            if (internalCall == false)
                _processedFields++;

            if (value == null || value is DynamicNullObject)
                return;

            _hadAnyNotNullValue = true;

            var lsv = value as LazyStringValue;
            if (lsv != null)
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = Hashing.XXHash64.Calculate(lsv.Buffer, (ulong)lsv.Size);
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer(lsv.Buffer, lsv.Size);
                        break;
                }

                return;
            }

            var s = value as string;
            if (s != null)
            {
                using (Slice.From(context, s, out Slice str))
                {
                    switch (_mode)
                    {
                        case Mode.SingleValue:
                            _singleValueHash = Hashing.XXHash64.Calculate(str.Content.Ptr, (ulong)str.Size);
                            break;
                        case Mode.MultipleValues:
                            CopyToBuffer(str.Content.Ptr, str.Size);
                            break;
                    }
                }

                return;
            }

            var lcsv = value as LazyCompressedStringValue;
            if (lcsv != null)
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = Hashing.XXHash64.Calculate(lcsv.Buffer, (ulong)lcsv.CompressedSize);
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer(lcsv.Buffer, lcsv.CompressedSize);
                        break;
                }

                return;
            }

            if (value is long l)
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        unchecked
                        {
                            _singleValueHash = (ulong)l;
                        }
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&l, sizeof(long));
                        break;
                }

                return;
            }

            if (value is decimal d)
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = Hashing.XXHash64.Calculate((byte*)&d, sizeof(decimal));
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&d, sizeof(decimal));
                        break;
                }

                return;
            }

            if (value is int num)
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = (ulong)num;
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&num, sizeof(int));
                        break;
                }

                return;
            }

            if (value is bool b)
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = b ? 0 : 1UL;
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&b, sizeof(bool));
                        break;
                }

                return;
            }

            if (value is double dbl)
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = (ulong)dbl;
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&d, sizeof(double));
                        break;
                }

                return;
            }

            if (value is LazyNumberValue lnv)
            {
                Process(context, lnv.Inner, true);
                return;
            }

            long? ticks = null;
            if (value is DateTime)
                ticks = ((DateTime)value).Ticks;
            if (value is DateTimeOffset)
                ticks = ((DateTimeOffset)value).Ticks;
            if (value is TimeSpan)
                ticks = ((TimeSpan)value).Ticks;

            if (ticks.HasValue)
            {
                var t = ticks.Value;
                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = (ulong)t;
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&t, sizeof(long));
                        break;
                }

                return;
            }

            if (value is BlittableJsonReaderObject json)
            {
                _mode = Mode.MultipleValues;

                if (_buffer == null)
                    _buffer = _buffersPool.Allocate(16);

                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < json.Count; i++)
                {
                    // this call ensures properties to be returned in the same order, regardless their storing order
                    json.GetPropertyByIndex(i, ref prop);

                    Process(context, prop.Value, true);
                }

                return;
            }

            if (value is IEnumerable enumerable)
            {
                _mode = Mode.MultipleValues;

                if (_buffer == null)
                    _buffer = _buffersPool.Allocate(16);

                foreach (var item in enumerable)
                {
                    Process(context, item, true);
                }

                return;
            }

            if (value is DynamicJsonValue djv)
            {
                _mode = Mode.MultipleValues;

                if (_buffer == null)
                    _buffer = _buffersPool.Allocate(16);

                foreach (var item in djv.Properties)
                {
                    Process(context, item.Value, true);
                }

                return;
            }

            throw new NotSupportedException($"Unhandled type: {value.GetType()}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CopyToBuffer(byte* value, int size)
        {
            if (_buffer == null || 
                _bufferPos + size > _buffer.SizeInBytes)
            {
                var newBuffer = _buffersPool.Allocate(Bits.NextPowerOf2(_bufferPos + size));
                if (_buffer != null)
                {
                    Memory.Copy(newBuffer.Address, _buffer.Address, _buffer.SizeInBytes);
                    _buffersPool.Return(_buffer);
                }
                _buffer = newBuffer;
            }

            Memory.Copy(_buffer.Address + _bufferPos, value, size);
            _bufferPos += size;
        }

        public enum Mode
        {
            SingleValue,
            MultipleValues
        }
        public struct Buffer
        {
            public byte* Address;
            public int Size;
        }

        public Buffer GetBuffer()
        {
            return new Buffer
            {
                Address = _buffer.Address,
                Size = _bufferPos
            };
        }

        public bool IsBufferSet => _buffer != null;

        public void ReleaseBuffer()
        {
            if (_buffer != null)
            {
                _buffersPool.Return(_buffer);
                _buffer = null;
            }
        }
    }
}
