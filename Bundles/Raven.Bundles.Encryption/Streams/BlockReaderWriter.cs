using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Encryption.Settings;

namespace Raven.Bundles.Encryption.Streams
{
	internal class BlockReaderWriter : IDisposable
	{
		private readonly EncryptionSettings settings;

		private readonly Stream stream;
		private readonly string key;
		private readonly object locker = new object();
		private readonly bool isReadonly;

		private readonly EncryptedFile.Header header;
		private EncryptedFile.Footer footer;

		public BlockReaderWriter(EncryptionSettings encryptionSettings, string key, Stream stream, int defaultBlockSize)
		{
			if (encryptionSettings == null)
				throw new ArgumentNullException("encryptionSettings");

			if (key == null)
				throw new ArgumentNullException("key");

			if (stream == null)
				throw new ArgumentNullException("stream");

			if (!stream.CanRead)
				throw new ArgumentException("The Underlying stream for a BlockReaderWriter must always be either read-only or read-write. Write only streams are not supported.");
			if (!stream.CanSeek)
				throw new ArgumentException("The Underlying stream for a BlockReaderWriter must be seekable.");

			isReadonly = !stream.CanWrite;

			this.settings = encryptionSettings;
			this.key = key;
			this.stream = stream;

			this.header = ReadOrWriteHeader(defaultBlockSize);
			this.footer = ReadOrWriteFooter();
		}

		public EncryptedFile.Header Header
		{
			get { return header; }
		}

		public EncryptedFile.Footer Footer
		{
			get { return footer; }
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

					var sizeTest = settings.Codec.EncodeBlock("Dummy key", new byte[defaultBlockSize]);

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

		private EncryptedFile.Footer ReadOrWriteFooter()
		{
			lock (locker)
			{
				if (stream.Length >= EncryptedFile.Header.HeaderSize + EncryptedFile.Footer.FooterSize)
				{
					// Read footer
					stream.Position = stream.Length - EncryptedFile.Footer.FooterSize;
					var footerBytes = stream.ReadEntireBlock(EncryptedFile.Footer.FooterSize);
					var footer = StructConverter.ConvertBitsToStruct<EncryptedFile.Footer>(footerBytes);

					// Sanity check that the footer has some value that could be correct
					var streamBlockLength = (stream.Length - (EncryptedFile.Header.HeaderSize + EncryptedFile.Footer.FooterSize)) / header.DiskBlockSize;
					var footerBlockLength = footer.TotalLength / header.DecryptedBlockSize;
					if (footerBlockLength != streamBlockLength - 1 && footerBlockLength != streamBlockLength)
						throw new InvalidDataException("File is corrupted: the length written to the file doesn't match the number of blocks in it.");

					return footer;
				}
				else
				{
					// Write footer
					stream.Position = EncryptedFile.Header.HeaderSize;
					var footer = new EncryptedFile.Footer { TotalLength = 0 };
					WriteFooterInCurrentPosition(footer);

					return footer;
				}
			}
		}

		private void WriteFooterInCurrentPosition(EncryptedFile.Footer footer)
		{
			var footerBytes = StructConverter.ConvertStructToBits(footer);

			if (!isReadonly)
				stream.Write(footerBytes, 0, EncryptedFile.Footer.FooterSize);
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

				long position = header.GetPhysicalPositionFromBlockNumber(blockNumber);
				if (stream.Length < position + header.IVSize + header.EncryptedBlockSize + EncryptedFile.Footer.FooterSize)
				{
					return new EncryptedFile.Block
					{
						BlockNumber = blockNumber,
						TotalStreamLength = footer.TotalLength,
						Data = new byte[header.DecryptedBlockSize],
					};
				}

				stream.Position = position;
				var iv = stream.ReadEntireBlock(header.IVSize);
				var encrypted = stream.ReadEntireBlock(header.EncryptedBlockSize);

				var decrypted = settings.Codec.DecodeBlock(key, new Codec.EncodedBlock(iv, encrypted));

				Debug.Assert(decrypted.Length == header.DecryptedBlockSize);

				return new EncryptedFile.Block
				{
					BlockNumber = blockNumber,
					TotalStreamLength = footer.TotalLength,
					Data = decrypted,
				};
			}
		}

		/// <summary>
		/// Writes a block to its correct place in the file.
		/// The block's TotalStreamLength is saved as the total length of the stream IF AND ONLY IF the written block is the last block in the stream.
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
				if (block.Data == null || block.Data.Length != header.DecryptedBlockSize)
					throw new ArgumentException("Block must have data with length == " + header.DecryptedBlockSize);

				long position = header.GetPhysicalPositionFromBlockNumber(block.BlockNumber);
				if (stream.Length - EncryptedFile.Footer.FooterSize < position)
				{
					throw new InvalidOperationException("Write past end of file.");
					//WriteEmptyBlocksUpTo(position);
				}

				stream.Position = position;
				var encrypted = settings.Codec.EncodeBlock(key, block.Data);

				Debug.Assert(encrypted.Data.Length == header.EncryptedBlockSize);

				stream.Write(encrypted.IV, 0, encrypted.IV.Length);
				stream.Write(encrypted.Data, 0, encrypted.Data.Length);

				if (stream.Length <= stream.Position + EncryptedFile.Footer.FooterSize)
				{
					footer.TotalLength = block.TotalStreamLength;
					WriteFooterInCurrentPosition(footer);
				}
			}
		}

		//private void WriteEmptyBlocksUpTo(long position)
		//{
		//	if (isReadonly)
		//		throw new InvalidOperationException("The current stream is read-only.");

		//	var emptyData = new byte[Header.DecryptedBlockSize];

		//	while (stream.Length < position)
		//	{
		//		var nextBlock = Header.GetBlockNumberFromPhysicalPosition(stream.Length);
		//		WriteBlock(new EncryptedFile.Block
		//		{
		//			BlockNumber = nextBlock,
		//			Data = emptyData
		//		});
		//	}
		//}

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
