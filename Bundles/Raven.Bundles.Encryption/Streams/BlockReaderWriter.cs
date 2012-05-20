using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.Encryption.Streams
{
	internal class BlockReaderWriter : IDisposable
	{
		public EncryptedFile.Header Header;
		private readonly Stream stream;
		private readonly string key;
		private readonly object locker = new object();

		public BlockReaderWriter(string key, Stream stream, int blockSize)
		{
			if (key == null)
				throw new ArgumentNullException("key");

			if (stream == null)
				throw new ArgumentNullException("stream");
			if (!stream.CanSeek || !stream.CanWrite)
				throw new ArgumentException("The underlying stream must be writable and seekable.");

			this.key = key;
			this.stream = stream;

			var ivSize = Codec.GetIVLength();

			this.Header = new EncryptedFile.Header
			{
				MagicNumber = EncryptedFile.DefaultMagicNumber,
				IVSize = ivSize,
				BlockSize = blockSize
			};
		}

		public void ReadHeader()
		{
			lock (locker)
			{
				lock (locker)
				{
					stream.Position = 0;
					var headerBytes = stream.ReadEntireBlock(EncryptedFile.Header.HeaderSize);

					Header = StructConverter.ConvertBitsToStruct<EncryptedFile.Header>(headerBytes);

					if (Header.MagicNumber != EncryptedFile.DefaultMagicNumber)
						throw new InvalidDataException("The magic number in the file doesn't match the expected magic number for encypted files. Perhaps this file isn't encrypted?");
				}
			}
		}

		/// <summary>
		/// Reads a block from its correct place in the file.
		/// </summary>
		public EncryptedFile.Block ReadBlock(long blockNumber)
		{
			lock (locker)
			{
				if (blockNumber < 0)
					throw new ArgumentOutOfRangeException("blockNumber");

				long position = Header.GetPhysicalPositionFromBlockNumber(blockNumber);
				if (stream.Length <= position)
				{
					return new EncryptedFile.Block
					{
						BlockNumber = blockNumber,
						Data = new byte[Header.BlockSize]
					};
				}

				stream.Position = position;
				var iv = stream.ReadEntireBlock(Header.IVSize);
				var encrypted = stream.ReadEntireBlock(Header.BlockSize);

				return new EncryptedFile.Block
				{
					BlockNumber = blockNumber,
					Data = Codec.DecodeBlock(key, new Codec.EncodedBlock(iv, encrypted))
				};
			}
		}

		public void WriteHeader()
		{
			lock (locker)
			{
				var headerBytes = StructConverter.ConvertStructToBits(Header);

				stream.Position = 0;
				stream.Write(headerBytes, 0, EncryptedFile.Header.HeaderSize);
			}
		}

		/// <summary>
		/// Writes a block to its correct place in the file.
		/// </summary>
		public void WriteBlock(EncryptedFile.Block block)
		{
			lock (locker)
			{
				if (block == null)
					throw new ArgumentNullException("block");
				if (block.BlockNumber < 0)
					throw new ArgumentOutOfRangeException("block", "Block number be non negative.");
				if (block.Data == null || block.Data.Length != Header.BlockSize)
					throw new ArgumentException("Block must have data with length == " + Header.BlockSize);

				long position = Header.GetPhysicalPositionFromBlockNumber(block.BlockNumber);
				if (stream.Length < position)
				{
					WriteEmptyBlocksUpTo(position);
				}

				stream.Position = position;
				var encrypted = Codec.EncodeBlock(key, block.Data);
				stream.Write(encrypted.IV, 0, encrypted.IV.Length);
				stream.Write(encrypted.Data, 0, encrypted.Data.Length);
			}
		}

		private void WriteEmptyBlocksUpTo(long position)
		{
			var emptyData = new byte[Header.BlockSize];

			while (stream.Length < position)
			{
				var nextBlock = Header.GetBlockNumberFromPhysicalPosition(stream.Length);
				WriteBlock(new EncryptedFile.Block
				{
					BlockNumber = nextBlock,
					Data = emptyData
				});
			}
		}

		public void Flush()
		{
			lock (locker)
			{
				stream.Flush();
			}
		}

		public void Dispose()
		{
			lock (locker)
			{
				stream.Dispose();
			}
		}
	}
}
