using System;
using System.Collections.Generic;
using System.Diagnostics;
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
		private readonly bool isReadonly;

		public BlockReaderWriter(string key, Stream stream, int defaultBlockSize)
		{
			if (key == null)
				throw new ArgumentNullException("key");

			if (stream == null)
				throw new ArgumentNullException("stream");

			if (!stream.CanRead)
				throw new ArgumentException("The Underlying stream for a BlockReaderWriter must always be either read-only or read-write. Write only streams are not supported.");
			if (!stream.CanSeek)
				throw new ArgumentException("The Underlying stream for a BlockReaderWriter must be seekable.");

			isReadonly = !stream.CanWrite;

			this.key = key;
			this.stream = stream;

			this.Header = ReadOrWriteHeader(defaultBlockSize);
		}

		private EncryptedFile.Header ReadOrWriteHeader(int defaultBlockSize)
		{
			lock (locker)
			{
				stream.Position = 0;

				if (stream.Length >= EncryptedFile.Header.HeaderSize)
				{
					// Read header

					var headerBytes = stream.ReadEntireBlock(EncryptedFile.Header.HeaderSize);
					var header = StructConverter.ConvertBitsToStruct<EncryptedFile.Header>(headerBytes);

					if (header.MagicNumber != EncryptedFile.DefaultMagicNumber)
						throw new InvalidDataException("The magic number in the file doesn't match the expected magic number for encypted files. Perhaps this file isn't encrypted?");

					return header;
				}
				else
				{
					// Write header

					var sizeTest = Codec.EncodeBlock("Dummy key", new byte[defaultBlockSize]);

					var header = new EncryptedFile.Header
					{
						MagicNumber = EncryptedFile.DefaultMagicNumber,
						DecryptedBlockSize = defaultBlockSize,
						IVSize = sizeTest.IV.Length,
						EncryptedBlockSize = sizeTest.Data.Length
					};
					var headerBytes = StructConverter.ConvertStructToBits(header);

					if (!isReadonly)
						stream.Write(headerBytes, 0, EncryptedFile.Header.HeaderSize);

					return header;
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
						Data = new byte[Header.DecryptedBlockSize]
					};
				}

				stream.Position = position;
				var iv = stream.ReadEntireBlock(Header.IVSize);
				var encrypted = stream.ReadEntireBlock(Header.EncryptedBlockSize);

				var decrypted = Codec.DecodeBlock(key, new Codec.EncodedBlock(iv, encrypted));

				Debug.Assert(decrypted.Length == Header.DecryptedBlockSize);

				return new EncryptedFile.Block
				{
					BlockNumber = blockNumber,
					Data = decrypted
				};
			}
		}

		/// <summary>
		/// Writes a block to its correct place in the file.
		/// </summary>
		public void WriteBlock(EncryptedFile.Block block)
		{
			if (isReadonly)
				throw new InvalidOperationException("The current stream is read-only.");

			lock (locker)
			{
				if (block == null)
					throw new ArgumentNullException("block");
				if (block.BlockNumber < 0)
					throw new ArgumentOutOfRangeException("block", "Block number be non negative.");
				if (block.Data == null || block.Data.Length != Header.DecryptedBlockSize)
					throw new ArgumentException("Block must have data with length == " + Header.DecryptedBlockSize);

				long position = Header.GetPhysicalPositionFromBlockNumber(block.BlockNumber);
				if (stream.Length < position)
				{
					WriteEmptyBlocksUpTo(position);
				}

				stream.Position = position;
				var encrypted = Codec.EncodeBlock(key, block.Data);

				Debug.Assert(encrypted.Data.Length == Header.EncryptedBlockSize);

				stream.Write(encrypted.IV, 0, encrypted.IV.Length);
				stream.Write(encrypted.Data, 0, encrypted.Data.Length);
			}
		}

		private void WriteEmptyBlocksUpTo(long position)
		{
			if (isReadonly)
				throw new InvalidOperationException("The current stream is read-only.");

			var emptyData = new byte[Header.DecryptedBlockSize];

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
