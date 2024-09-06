using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Utils;
using static Sparrow.PortableExceptions;

namespace Voron
{
    public unsafe struct ValueReader(byte* val, int len) 
    {
        [ThreadStatic]
        private static byte[] _tmpBuf;
        static ValueReader()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _tmpBuf = null;
        }

        private int _pos = 0;
        private readonly int _len = len;

        public int Current
        {
            get => _pos;
        }

        public byte* Base { get; } = val;


        public int Length { get; } = len;

        public void Reset()
        {
            _pos = 0;
        }

        public Stream AsStream()
        {
            return new UnmanagedMemoryStream(Base, _len, _len, FileAccess.Read);
        }

        public void Skip(int size)
        {
            _pos += size;
        }

        public int Read(Span<byte> buffer, int offset, int count)
        {
            return Read(buffer.Slice(offset), count);
        }

        public int Read(Span<byte> buffer, int count)
        {
            count = Math.Min(count, _len - _pos);
            if (count <= 0)
                return 0;
            
            Unsafe.CopyBlock(ref buffer[0], ref Unsafe.AsRef<byte>(Base + _pos), (uint)count);
            _pos += count;
            
            return count;
        }

        public int Read(byte* buffer, int count)
        {
            count = Math.Min(count, _len - _pos);
            if (count <= 0)
                return 0;
            Memory.Copy(buffer, Base + _pos, count);
            _pos += count;

            return count;
        }

        public T Read<T>() where T : unmanaged
        {
            ThrowIf<EndOfStreamException>(_len - _pos < Unsafe.SizeOf<T>());

            var val = *(T*)(Base + _pos);
            _pos += Unsafe.SizeOf<T>();
            return val;
        }

        public T ReadBigEndian<T>() where T : unmanaged
        {
            ThrowIf<EndOfStreamException>(_len - _pos < Unsafe.SizeOf<T>());
            
            var val = *(T*)(Base + _pos);
            _pos += Unsafe.SizeOf<T>();
            
            if (typeof(T) == typeof(long))
            {
                return (T)(object)Bits.SwapBytes((long)(object)val);
            }
            if (typeof(T) == typeof(int))
            {
                return (T)(object)Bits.SwapBytes((int)(object)val);
            }
            if (typeof(T) == typeof(byte))
            {
                return val;
            }

            throw new NotSupportedException($"Type '{typeof(T).Name}' is not supported.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte[] EnsureTempBuffer(int size)
        {
            if (_tmpBuf != null && _tmpBuf.Length >= size)
                return _tmpBuf;

            return _tmpBuf = new byte[Bits.PowerOf2(size)];
        }

        public string ReadString(int length)
        {
            var arraySegment = ReadBytes(length);
            return Encodings.Utf8.GetString(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
        }

        public string ToStringValue()
        {
            int length = _len - _pos;
            var arraySegment = ReadBytes(length);
            return Encodings.Utf8.GetString(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
        }

        public override string ToString()
        {
            var old = _pos;
            var stringValue = ToStringValue();
            _pos = old;
            return stringValue;
        }

        public ArraySegment<byte> ReadBytes(int length)
        {
            int size = Math.Min(length, _len - _pos);
            var buffer = EnsureTempBuffer(length);
            var used = Read(buffer, size);
            return new ArraySegment<byte>(buffer, 0, used);
        }

        public void CopyTo(Stream stream)
        {
            var buffer = new byte[4096];
            while (true)
            {
                int read = Read(buffer, buffer.Length);
                if (read == 0)
                    return;

                stream.Write(buffer, 0, read);
            }
        }

        public int CompareTo(ValueReader other)
        {
            int size = Math.Min(Length, other.Length);

            int r = Memory.CompareInline(Base, other.Base, size);

            if (r != 0)
                return r;

            return Length - other.Length;
        }

        public ByteStringContext.InternalScope AsSlice(ByteStringContext context, out Slice str)
        {
            if (_len >= ushort.MaxValue)
                throw new InvalidOperationException("Cannot convert to slice, len is too big: " + _len);

            return Slice.From(context, Base, _len, out str);
        }

        public Span<byte> AsSpan()
        {
            return new Span<byte>(Base, _len);
        }
    }
}
