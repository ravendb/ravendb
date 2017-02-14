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
using System.Text;

namespace Raven.Client.Extensions.Streams
{
    /// <summary>
    /// Extensions for working with streams
    /// </summary>
    internal static class StreamExtensions
    {
        public static void Write(this Stream stream, int value)
        {
            var buffer = BitConverter.GetBytes(value);
            stream.Write(buffer, 0, buffer.Length);
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
    }
}
