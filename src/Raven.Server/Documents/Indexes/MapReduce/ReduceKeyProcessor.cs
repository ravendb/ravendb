using System;
using System.Runtime.CompilerServices;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class ReduceKeyProcessor
    {
        private readonly UnmanagedBuffersPoolWithLowMemoryHandling _buffersPool;
        private Mode _mode;
        private AllocatedMemoryData _buffer;
        private int _bufferPos;
        private ulong _singleValueHash;

        public ReduceKeyProcessor(int numberOfReduceFields, UnmanagedBuffersPoolWithLowMemoryHandling buffersPool)
        {
            _buffersPool = buffersPool;
            if (numberOfReduceFields == 1)
            {
                _mode = Mode.SingleValue;
            }
            else
            {
                _mode = Mode.MultipleValues;
                _buffer = _buffersPool.Allocate(16);
                _bufferPos = 0;
            }
        }

        public void Reset()
        {
            _bufferPos = 0;
        }

        public ulong Hash
        {
            get
            {
                switch (_mode)
                {
                    case Mode.SingleValue:
                        return _singleValueHash;
                    case Mode.MultipleValues:
                        return Hashing.XXHash64.CalculateInline((byte*)_buffer.Address, (ulong)_bufferPos);
                    default:
                        throw new NotSupportedException($"Unknown reduce value processing mode: {_mode}");
                }
            }
        }

        public void Process(object value)
        {
            if (value == null || value is DynamicNullObject)
                return;

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
                fixed (char* p = s)
                {
                    switch (_mode)
                    {
                        case Mode.SingleValue:
                            _singleValueHash = Hashing.XXHash64.Calculate((byte*)p, (ulong)s.Length * sizeof(char));
                            break;
                        case Mode.MultipleValues:
                            CopyToBuffer((byte*)p, s.Length * sizeof(char));
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

            if (value is long)
            {
                var l = (long)value;

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

            if (value is decimal)
            {
                var d = (decimal)value;

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

            if (value is int)
            {
                var i = (int)value;

                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = (ulong)i;
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&i, sizeof(int));
                        break;
                }

                return;
            }

            if (value is double)
            {
                var d = (double)value;

                switch (_mode)
                {
                    case Mode.SingleValue:
                        _singleValueHash = (ulong)d;
                        break;
                    case Mode.MultipleValues:
                        CopyToBuffer((byte*)&d, sizeof(double));
                        break;
                }

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

            var dynamicJson = value as DynamicBlittableJson;

            if (dynamicJson != null)
            {
                var obj = dynamicJson.BlittableJson;

                _mode = Mode.MultipleValues;

                if (_buffer == null)
                    _buffer = _buffersPool.Allocate(16);
                
                for (int i = 0; i < obj.Count; i++)
                {
                    // this call ensures properties to be returned in the same order, regardless their storing order
                    var property = obj.GetPropertyByIndex(i); 
                    
                    Process(property.Item2);
                }
                
                return;
            }

            var dynamicArray = value as DynamicArray;

            if (dynamicArray != null)
            {
                _mode = Mode.MultipleValues;

                if (_buffer == null)
                    _buffer = _buffersPool.Allocate(16);
                
                foreach (var item in dynamicArray)
                {
                    Process(item);
                }

                return;
            }

            throw new NotSupportedException($"Unhandled type: {value.GetType()}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CopyToBuffer(byte* value, int size)
        {
            if (_bufferPos + size > _buffer.SizeInBytes)
            {
                var newBuffer = _buffersPool.Allocate(Bits.NextPowerOf2(_bufferPos + size));
                Memory.Copy((byte*)newBuffer.Address, (byte*)_buffer.Address, _buffer.SizeInBytes);

                _buffersPool.Return(_buffer);
                _buffer = newBuffer;
            }

            Memory.Copy((byte*)_buffer.Address + _bufferPos, value, size);
            _bufferPos += size;
        }

        enum Mode
        {
            SingleValue,
            MultipleValues
        }
    }
}