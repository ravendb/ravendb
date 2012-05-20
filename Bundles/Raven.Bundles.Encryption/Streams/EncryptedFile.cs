using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Bundles.Encryption.Streams
{
	/// <summary>
	/// Support class for things common to the EncryptedInputStream and EncryptedOutputStream.
	/// 
	/// Logical positions are positions as reported to the stream reader/writer.
	/// Physical positions are the positions on disk.
	/// </summary>
	internal static class EncryptedFile
	{
		public const ulong DefaultMagicNumber = 0x4372797074656420; // "Crypted "

		[StructLayout(LayoutKind.Sequential, Pack = 1)]
		internal struct Header
		{
			public ulong MagicNumber;
			public int IVSize;
			public int BlockSize;

			public static readonly int HeaderSize = Marshal.SizeOf(typeof(Header));

			public int DiskBlockSize
			{
				get { return BlockSize + IVSize; }
			}

			public long GetBlockNumberFromPhysicalPosition(long position)
			{
				return (position - HeaderSize) / DiskBlockSize;
			}

			public long GetBlockNumberFromLogicalPosition(long position)
			{
				return position / BlockSize;
			}

			public long GetPhysicalPositionFromBlockNumber(long number)
			{
				return DiskBlockSize * number + HeaderSize;
			}

			public long GetLogicalPositionFromBlockNumber(long number)
			{
				return BlockSize * number;
			}

			public long GetBlockOffsetFromLogicalPosition(long position)
			{
				var blockNumber = GetBlockNumberFromLogicalPosition(position);
				var blockStart = GetLogicalPositionFromBlockNumber(blockNumber);
				return position - blockStart;
			}
		}

		public class Block
		{
			public long BlockNumber;
			public byte[] Data;
		}
	}
}