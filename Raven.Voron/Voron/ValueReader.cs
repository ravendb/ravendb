using Sparrow;
using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Voron.Impl;
using Voron.Util;
using Voron.Util.Conversion;

namespace Voron
{
    public unsafe struct ValueReader
    {
        [ThreadStatic]
        private static byte[] tmpBuf;

        [ThreadStatic]
        private static byte[] smallTempBuffer;
        
        private int _pos;
        private readonly byte[] _buffer;
        
        private readonly int _len;
        private readonly byte* _val;        

        public byte* Base { get { return _val; } }

        public ValueReader(Stream stream)
        {
            long position = stream.Position;

            _len = (int) (stream.Length - stream.Position);
            _buffer = new byte[_len];

            int pos = 0;
            while (true)
            {
                int read = stream.Read(_buffer, pos, _buffer.Length - pos);
                if (read == 0)
                    break;
                pos += read;
            }
            stream.Position = position;

            _pos = 0;
            _val = null;            
        }

        public ValueReader(byte[] array, int len)
        {
            if (array == null) throw new ArgumentNullException("array");
            _buffer = array;
            _len = len;
            _pos = 0;
            _val = null;
        }

        public ValueReader(byte* val, int len)
        {
            _val = val;
            _len = len;
            _pos = 0;
            _buffer = null;
        }

        public int Length
        {
            get { return _len; }
        }

        public bool EndOfData
        {
            get { return _len == _pos; }
        }

        public void Reset()
        {
            _pos = 0;
        }

        public Stream AsStream()
        {
            if (_val == null)
                return new MemoryStream(_buffer, false);

            return new UnmanagedMemoryStream(_val, _len, _len, FileAccess.Read);
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            fixed (byte* b = buffer)
                return Read(b + offset, count);
        }

        public int Read(byte* buffer, int count)
        {
            count = Math.Min(count, _len - _pos);
            if (count <= 0)
                return 0;

            if (_val == null)
            {
                fixed (byte* b = _buffer)
                    Memory.Copy(buffer, b + _pos, count);
            }
            else
            {
                Memory.Copy(buffer, _val + _pos, count);
            }
            _pos += count;

            return count;
        }

        public int ReadLittleEndianInt32()
        {
            if (_len - _pos < sizeof(int))
                throw new EndOfStreamException();

            EnsureSmallTempBuffer();

            fixed (byte* tmpBuffer = smallTempBuffer)
            {
                if (_val == null)
                {
                    fixed (byte* b = _buffer)
                    {
                        *(int*)tmpBuffer = *(int*)(b + _pos);
                    }
                }
                else
                {
                    *(int*)tmpBuffer = *(int*)(_val + _pos);
                }

                _pos += sizeof(int);

                if (!BitConverter.IsLittleEndian)
                    return SwapBitShift(*(int*)tmpBuffer);

                return *(int*)tmpBuffer;
            }


        }


        public long ReadLittleEndianInt64()
        {
            if (_len - _pos < sizeof (long))
                throw new EndOfStreamException();

            EnsureSmallTempBuffer();

            fixed (byte* tmpBuffer = smallTempBuffer)
            {
                if (_val == null)
                {
                    fixed (byte* b = _buffer)
                    {
                        *(long*)tmpBuffer = *(long*)(b + _pos);
                    }
                }
                else
                {
                    *(long*)tmpBuffer = *(long*)(_val + _pos);
                }

                _pos += sizeof(long);

                if (!BitConverter.IsLittleEndian)
                    return SwapBitShift(*(long*)tmpBuffer);

                return *(long*)tmpBuffer;
            }
        }

        public int ReadBigEndianInt32()
        {
            if (_len - _pos < sizeof(int))
                throw new EndOfStreamException();

            EnsureSmallTempBuffer();

            fixed (byte* tmpBuffer = smallTempBuffer)
            {
                if (_val == null)
                {
                    fixed (byte* b = _buffer)
                    {
                        *(int*)tmpBuffer = *(int*)(b + _pos);
                    }
                }
                else
                {
                    *(int*)tmpBuffer = *(int*)(_val + _pos);
                }

                _pos += sizeof(int);

                if (BitConverter.IsLittleEndian)
                    return SwapBitShift(*(int*)tmpBuffer);

                return *(int*)tmpBuffer;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long SwapBitShift(long value)
        {
            ulong uvalue = (ulong)value;
            ulong swapped = (0x00000000000000FF) & (uvalue >> 56) |
                            (0x000000000000FF00) & (uvalue >> 40) |
                            (0x0000000000FF0000) & (uvalue >> 24) |
                            (0x00000000FF000000) & (uvalue >> 8)  |
                            (0x000000FF00000000) & (uvalue << 8)  |
                            (0x0000FF0000000000) & (uvalue << 24) |
                            (0x00FF000000000000) & (uvalue << 40) |
                            (0xFF00000000000000) & (uvalue << 56);
            return (long) swapped;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SwapBitShift(int value)
        {
            uint uvalue = (uint)value;
            uint swapped = ((0x000000FF) & (uvalue >> 24) |
                            (0x0000FF00) & (uvalue >> 8) |
                            (0x00FF0000) & (uvalue << 8) |
                            (0xFF000000) & (uvalue << 24));
            return (int)swapped;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]                             
        private static short SwapBitShift(short value)
        {
            uint uvalue = (uint)value;
            uint swapped = (0x000000FF) & (uvalue << 8) |
                           (0x0000FF00) & (uvalue >> 8);
            return (short)swapped;
        }



        public long ReadBigEndianInt64()
        {
            if (_len - _pos < sizeof(long))
                throw new EndOfStreamException();

            EnsureSmallTempBuffer();

            fixed (byte* tmpBuffer = smallTempBuffer)
            {
                if (_val == null)
                {
                    fixed (byte* b = _buffer)
                    {
                        *(long*)tmpBuffer = *(long*)(b + _pos);
                    }
                }
                else
                {
                    *(long*)tmpBuffer = *(long*)(_val + _pos);
                }

                _pos += sizeof(long);

                if (BitConverter.IsLittleEndian)
                    return SwapBitShift(*(long*)tmpBuffer);

                return *(long*)tmpBuffer;
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte[] EnsureTempBuffer(int size)
        {
            if (tmpBuf != null && tmpBuf.Length >= size)
                return tmpBuf;

            return tmpBuf = new byte[Utils.NearestPowerOfTwo(size)];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureSmallTempBuffer()
        {
            if (smallTempBuffer == null)
                smallTempBuffer = new byte[sizeof(long)];            
        }

        public string ToStringValue()
        {
            int used;
            int length = _len - _pos;
            return Encoding.UTF8.GetString(ReadBytes(length, out used), 0, used);
        }

        public override string ToString()
        {
            var old = _pos;
            var stringValue = ToStringValue();
            _pos = old;
            return stringValue;
        }

        public byte[] ReadBytes(int length, out int used)
        {
            int size = Math.Min(length, _len - _pos);
            var buffer = EnsureTempBuffer(length);
            used = Read(buffer, 0, size);
            return buffer;
        }

        public void CopyTo(Stream stream)
        {
            var buffer = new byte[4096];
            while (true)
            {
                int read = Read(buffer, 0, buffer.Length);
                if (read == 0)
                    return;

                stream.Write(buffer, 0, read);
            }
        }

        public int CompareTo(ValueReader other)
        {
            int r = CompareData(other, Math.Min(Length, other.Length));
            if (r != 0)
                return r;

            return Length - other.Length;
        }

        private int CompareData(ValueReader other, int len)
        {
            if (_buffer != null)
            {
                fixed (byte* a = _buffer)
                {
                    if (other._buffer != null)
                    {
                        fixed (byte* b = other._buffer)
                        {
                            return Memory.Compare(a, b, len);
                        }
                    }
                    return Memory.Compare(a, other._val, len);
                }
            }

            if (other._buffer != null)
            {
                fixed (byte* b = other._buffer)
                {
                    return Memory.Compare(_val, b, len);
                }
            }

            return Memory.Compare(_val, other._val, len);
        }

        public Slice AsSlice()
        {
            if (_len >= ushort.MaxValue)
                throw new InvalidOperationException("Cannot convert to slice, len is too big: " + _len);
            
            if (_buffer != null)
                return new Slice(_buffer, (ushort) _len);

            return new Slice(_val, (ushort) _len);
        }
    }
}
