//-----------------------------------------------------------------------
// <copyright file="StreamExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Logging;
using System.Runtime.InteropServices;
using System.Text;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Util.Streams;

namespace Raven.NewClient.Abstractions.Extensions
{
    /// <summary>
    /// Extensions for working with streams
    /// </summary>
    public static class StreamExtensions
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(StreamExtensions));

        public static void CopyTo(this Stream stream, Stream other)
        {
            var buffer = BufferSharedPools.ByteArray.Allocate();

            try
            {
                while (true)
                {
                    int read = stream.Read(buffer, 0, buffer.Length);
                    if (read == 0)
                        return;
                    other.Write(buffer, 0, read);
                }
            }
            finally
            {
                BufferSharedPools.ByteArray.Free(buffer);
            }
        }

        public static void Write(this Stream stream, long value)
        {
            var buffer = BitConverter.GetBytes(value);
            stream.Write(buffer, 0, buffer.Length);
        }

        public static void Write(this Stream stream, int value)
        {
            var buffer = BitConverter.GetBytes(value);
            stream.Write(buffer, 0, buffer.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void PartialRead(this Stream stream, byte[] buffer, int size)
        {
            var totalRead = 0;
            while (totalRead < size)
            {
                var bytesRead = stream.Read(buffer, totalRead, size - totalRead);
                if (bytesRead == 0)
                    throw new EndOfStreamException();
                totalRead += bytesRead;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReadInt64(this Stream stream)
        {
            var buffer = new byte[sizeof(long)];
            var bytesRead = stream.Read(buffer, 0, sizeof(long));
            if (bytesRead == 0)
                throw new EndOfStreamException();
            return BitConverter.ToInt64(buffer, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong ReadUInt64(this Stream stream)
        {
            var buffer = new byte[sizeof(ulong)];
            var bytesRead = stream.Read(buffer, 0, sizeof(ulong));
            if (bytesRead == 0)
                throw new EndOfStreamException();
            return BitConverter.ToUInt64(buffer, 0);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadInt32(this Stream stream)
        {
            var buffer = new byte[sizeof(int)];
            var bytesRead = stream.Read(buffer, 0, sizeof(int));
            if (bytesRead == 0)
                throw new EndOfStreamException();
            return BitConverter.ToInt32(buffer, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ReadString(this Stream stream)
        {
            return ReadString(stream, Encoding.UTF8);
        }

        public static string ReadString(this Stream stream, Encoding encoding)
        {
            var stringLength = stream.ReadInt32();            
            var buffer = new byte[stringLength];
            var bytesRead = stream.Read(buffer, 0, stringLength);
            if (bytesRead == 0)
                throw new EndOfStreamException();
            return encoding.GetString(buffer, 0, stringLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ReadStringWithoutPrefix(this Stream stream)
        {
            return ReadStringWithoutPrefix(stream, Encoding.UTF8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ReadStringWithoutPrefix(this Stream stream, Encoding encoding)
        {
            var buffer = stream.ReadData();

            return encoding.GetString(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Write(this Stream stream, string value)
        {
            Write(stream, value, Encoding.UTF8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Write(this Stream stream, string value, Encoding encoding)
        {
            var buffer = encoding.GetBytes(value);
            stream.Write(buffer.Length);
            stream.Write(buffer, 0, buffer.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Write(this Stream stream, long? etag)
        {
            var buffer = BitConverter.GetBytes(etag.Value);
            stream.Write(buffer, 0, buffer.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long? ReadEtag(this Stream stream)
        {
            const int EtagSize = 8;

            var buffer = new byte[EtagSize]; //etag size is 16 bytes
            var bytesRead = stream.Read(buffer, 0, EtagSize);
            if (bytesRead == 0)
                throw new EndOfStreamException();
            return BitConverter.ToInt64(buffer, 0);
        }

        /// <summary>
        /// Reads the entire request buffer to memory and return it as a byte array.
        /// </summary>
        /// <param name="stream">The stream to read.</param>
        /// <returns>The returned byte array.</returns>
        public static byte[] ReadData(this Stream stream)
        {
            var list = new List<byte[]>();

            var buffer = BufferSharedPools.ByteArray.Allocate();

            try
            {
                var currentOffset = 0;
                int read;

                while ((read = stream.Read(buffer, currentOffset, buffer.Length - currentOffset)) != 0)
                {
                    currentOffset += read;
                    if (currentOffset == buffer.Length)
                    {
                        list.Add(buffer);
                        buffer = BufferSharedPools.ByteArray.Allocate();
                        currentOffset = 0;
                    }
                }

                var totalSize = list.Sum(x => x.Length) + currentOffset;

                var result = new byte[totalSize];
                var resultOffset = 0;
                foreach (var partial in list)
                {
                    Buffer.BlockCopy(partial, 0, result, resultOffset, partial.Length);
                    resultOffset += partial.Length;
                }

                Buffer.BlockCopy(buffer, 0, result, resultOffset, currentOffset);
                return result;
            }
            finally
            {
                foreach (var partial in list)
                    BufferSharedPools.ByteArray.Free(partial);

                BufferSharedPools.ByteArray.Free(buffer);
            }
        }

        /// <summary>
        /// Asynchronously reads the entire request buffer to memory and return it as a byte array.
        /// </summary>
        /// <param name="stream">The stream to read.</param>
        /// <returns>A task that, when completed, contains the returned byte array.</returns>
        public static async Task<byte[]> ReadDataAsync(this Stream stream)
        {
            var list = new List<byte[]>();

            var buffer = BufferSharedPools.ByteArray.Allocate();

            try
            {
                var currentOffset = 0;
                int read;
                while ((read = await stream.ReadAsync(buffer, currentOffset, buffer.Length - currentOffset).ConfigureAwait(false)) != 0)
                {
                    currentOffset += read;
                    if (currentOffset == buffer.Length)
                    {
                        list.Add(buffer);
                        buffer = BufferSharedPools.ByteArray.Allocate();
                        currentOffset = 0;
                    }
                }
                var totalSize = list.Sum(x => x.Length) + currentOffset;
                var result = new byte[totalSize];
                var resultOffset = 0;
                foreach (var partial in list)
                {
                    Buffer.BlockCopy(partial, 0, result, resultOffset, partial.Length);
                    resultOffset += partial.Length;
                }
                Buffer.BlockCopy(buffer, 0, result, resultOffset, currentOffset);
                return result;
            }
            finally
            {
                foreach (var partial in list)
                    BufferSharedPools.ByteArray.Free(partial);

                BufferSharedPools.ByteArray.Free(buffer);
            }
        }

        /// <summary>
        /// Allocates a byte array and reads an entire block from the stream
        /// </summary>
        public static byte[] ReadEntireBlock(this Stream stream, int count)
        {
            var buffer = new byte[count];
            stream.ReadEntireBlock(buffer, 0, count);

            return buffer;
        }

        /// <summary>
        /// Reads an entire block from the stream
        /// </summary>
        public static void ReadEntireBlock(this Stream stream, byte[] buffer, int start, int count)
        {
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = stream.Read(buffer, start + totalRead, count - totalRead);
                if (read == 0)
                    throw new EndOfStreamException();
                totalRead += read;
            }
        }

        public static Stream DisposeTogetherWith(this Stream stream, params IDisposable[] disposables)
        {
            return new DisposingStream(stream, disposables);
        }

        private class DisposingStream : Stream
        {
            private readonly Stream stream;
            private readonly IDisposable[] disposables;

            public DisposingStream(Stream stream, IDisposable[] disposables)
            {
                this.stream = stream;
                this.disposables = disposables;
            }

            public override bool CanRead
            {
                get { return stream.CanRead; }
            }

            public override bool CanSeek
            {
                get { return stream.CanSeek; }
            }

            public override bool CanWrite
            {
                get { return stream.CanWrite; }
            }

            public override void Flush()
            {
                stream.Flush();
            }

            public override long Length
            {
                get { return stream.Length; }
            }

            public override long Position
            {
                get { return stream.Position; }
                set { stream.Position = value; }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return stream.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                return stream.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                stream.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                stream.Write(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                stream.Dispose();
                if (disposing)
                {
                    foreach (var d in disposables)
                    {
                        try
                        {
                            d.Dispose();
                        }
                        catch (Exception ex)
                        {
                            Logger.ErrorException("Error when disposing a DisposingStream: " + ex.Message, ex);
                        }
                    }
                }
            }
        }
    }
}
