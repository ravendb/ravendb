using Sparrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util.Streams
{
    public static class BufferSharedPools
    {
        private const int Megabyte = 1024 * 1024;
        private const int Kilobyte = 1024;

        public const int HugeByteBufferSize = 4 * Megabyte;
        public const int BigByteBufferSize = 512 * Kilobyte;
        public const int ByteBufferSize = 64 * Kilobyte;
        public const int SmallByteBufferSize = 4 * Kilobyte;
        public const int MicroByteBufferSize = Kilobyte / 2;

        /// <summary>
        /// Used to reduce the # of temporary byte[]s created to satisfy serialization and
        /// other I/O requests
        /// </summary>
        public static readonly ObjectPool<byte[]> HugeByteArray = new ObjectPool<byte[]>(() => new byte[HugeByteBufferSize], 30);

        /// <summary>
        /// Used to reduce the # of temporary byte[]s created to satisfy serialization and
        /// other I/O requests
        /// </summary>
        public static readonly ObjectPool<byte[]> BigByteArray = new ObjectPool<byte[]>(() => new byte[BigByteBufferSize], 50);

        /// <summary>
        /// Used to reduce the # of temporary byte[]s created to satisfy serialization and
        /// other I/O requests
        /// </summary>
        public static readonly ObjectPool<byte[]> ByteArray = new ObjectPool<byte[]>(() => new byte[ByteBufferSize], 100);

        /// <summary>
        /// Used to reduce the # of temporary byte[]s created to satisfy serialization and
        /// other I/O requests
        /// </summary>
        public static readonly ObjectPool<byte[]> SmallByteArray = new ObjectPool<byte[]>(() => new byte[SmallByteBufferSize], 100);


        /// <summary>
        /// Used to reduce the # of temporary byte[]s created to satisfy serialization and
        /// other I/O requests
        /// </summary>
        public static readonly ObjectPool<byte[]> MicroByteArray = new ObjectPool<byte[]>(() => new byte[MicroByteBufferSize], 100);

    }
}
