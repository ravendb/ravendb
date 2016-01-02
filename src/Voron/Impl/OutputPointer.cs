// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Bond.IO.Unsafe
{
    using Bond.Protocols;
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Text;

    /// <summary>
    /// Implements IOutputStream on top of unmanaged memory buffer
    /// </summary>
    public sealed unsafe class OutputPointer : IOutputStream
    {
        const int BlockCopyMin = 32;
        readonly byte* buffer;
        readonly int length;
        int position;

        /// <summary>
        /// Gets data inside the buffer
        /// </summary>
        public IntPtr Data
        {
            get { return (IntPtr)buffer; }
        }

        public OutputPointer(IntPtr buffer, int length)
            : this((byte*)buffer, length)
        { }

        public OutputPointer(byte* buffer, int length)
        {
            Debug.Assert(BitConverter.IsLittleEndian);

            this.buffer = buffer;
            this.length = length;
            position = 0;
        }

        #region IOutputStream

        /// <summary>
        /// Gets or sets the current position within the buffer
        /// </summary>
        public long Position
        {
            get { return position; }
            set { position = checked((int)value); }
        }

        /// <summary>
        /// Write 8-bit unsigned integer
        /// </summary>
        public void WriteUInt8(byte value)
        {
            if (position >= length)
            {
                EndOfStream(sizeof(byte));
            }

            buffer[position++] = value;
        }

        /// <summary>
        /// Write little-endian encoded 16-bit unsigned integer
        /// </summary>
        public void WriteUInt16(ushort value)
        {
            if (position + sizeof(ushort) > length)
            {
                EndOfStream(sizeof(ushort));
            }

            *((ushort*)(buffer + position)) = value;
            position += sizeof(ushort);
        }

        /// <summary>
        /// Write little-endian encoded 32-bit unsigned integer
        /// </summary>
        public void WriteUInt32(uint value)
        {
            if (position + sizeof(uint) > length)
            {
                EndOfStream(sizeof(uint));
            }

            *((uint*)(buffer + position)) = value;
            position += sizeof(uint);
        }

        /// <summary>
        /// Write little-endian encoded 64-bit unsigned integer
        /// </summary>
        public void WriteUInt64(ulong value)
        {
            if (position + sizeof(ulong) > length)
            {
                EndOfStream(sizeof(ulong));
            }

            *((ulong*)(buffer + position)) = value;
            position += sizeof(ulong);
        }

        /// <summary>
        /// Write little-endian encoded single precision ‎IEEE 754 float
        /// </summary>
        public void WriteFloat(float value)
        {
            if (position + sizeof(float) > length)
            {
                EndOfStream(sizeof(float));
            }

            *((float*)(buffer + position)) = value;
            position += sizeof(float);
        }

        /// <summary>
        /// Write little-endian encoded double precision ‎IEEE 754 float
        /// </summary>
        public void WriteDouble(double value)
        {
            if (position + sizeof(double) > length)
            {
                EndOfStream(sizeof(double));
            }

            *((double*)(buffer + position)) = value;
            position += sizeof(double);
        }

        /// <summary>
        /// Write an array of bytes verbatim
        /// </summary>
        /// <param name="bytes">Array segment specifying bytes to write</param>
        public void WriteBytes(ArraySegment<byte> bytes)
        {
            var newOffset = position + bytes.Count;
            if (newOffset > length)
            {
                EndOfStream(bytes.Count);
            }

            if (bytes.Count < BlockCopyMin)
            {
                for (int i = position, j = bytes.Offset; i < newOffset; ++i, ++j)
                {
                    buffer[i] = bytes.Array[j];
                }
            }
            else
            {
                Marshal.Copy(bytes.Array, bytes.Offset, (IntPtr)(buffer + position), bytes.Count);
            }

            position = newOffset;
        }

        /// <summary>
        /// Write variable encoded 16-bit unsigned integer
        /// </summary>
        public void WriteVarUInt16(ushort value)
        {
            if (position + IntegerHelper.MaxBytesVarInt16 > length)
            {
                EndOfStream(IntegerHelper.MaxBytesVarInt16);
            }
            position = IntegerHelper.EncodeVarUInt16(buffer, value, position);
        }

        /// <summary>
        /// Write variable encoded 32-bit unsigned integer
        /// </summary>
        public void WriteVarUInt32(uint value)
        {
            if (position + IntegerHelper.MaxBytesVarInt32 > length)
            {
                EndOfStream(IntegerHelper.MaxBytesVarInt32);
            }
            position = IntegerHelper.EncodeVarUInt32(buffer, value, position);
        }

        /// <summary>
        /// Write variable encoded 64-bit unsigned integer
        /// </summary>
        public void WriteVarUInt64(ulong value)
        {
            if (position + IntegerHelper.MaxBytesVarInt64 > length)
            {
                EndOfStream(IntegerHelper.MaxBytesVarInt64);
            }
            position = IntegerHelper.EncodeVarUInt64(buffer, value, position);
        }

        /// <summary>
        /// Write UTF-8 or UTF-16 encoded string
        /// </summary>
        /// <param name="encoding">String encoding</param>
        /// <param name="value">String value</param>
        /// <param name="size">Size in bytes of encoded string</param>
        public void WriteString(Encoding encoding, string value, int size)
        {
            if (position + size > length)
            {
                EndOfStream(size);
            }

            fixed (char* valuePtr = value)
            {
                position += encoding.GetBytes(valuePtr, value.Length, buffer + position, length - position);
            }
        }

        #endregion IOutputStream

        static void EndOfStream(int count)
        {
            throw new EndOfStreamException();
        }
    }
}