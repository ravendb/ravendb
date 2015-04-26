using System;
using System.IO;
using Raven.Bundles.Encryption.Settings;
using Raven.Abstractions.Data;

namespace Raven.Bundles.Encryption.Streams
{
	/// <summary>
	/// Wraps a FileStream (or other seekable stream) with an encryptor and decryptor.
	/// This is like a Seekable CryptoStream.
	/// 
	/// The file format is as follows:
	/// Header:
	///		- Magic: A UInt64 magic number to recognize that this is indeed an encrypted stream.
	///		- IV: An Int32 for IV size in bytes
	///		- BlockSize: An Int32 for unencrypted block size in bytes.
	///		- EncryptedBlockSize: An Int32 for encrypted block size in bytes.
	///	Block:
	///		- BlockIV: byte[], Length == Header.IV
	///		- Data: byte[], Length == Header.BlockSize
	/// Footer:
	///		- Length: After the last block, an Int64 length is written. This is the total length of the file.
	///				  the length is required, because the blocks are of an all identical size. If the file ends
	///				  in the middle of a block, this length will be used to truncate that block after decryption.
	/// </summary>
	public class SeekableCryptoStream : Stream
	{
		private readonly BlockReaderWriter underlyingStream;
		private readonly int currentBlockSize;
		private readonly object locker = new object();
		private EncryptedFile.Block currentReadingBlock;
		private EncryptedFile.Block currentWritingBlock;
	    private long currentWritingBlockUsage;
		private long currentPosition;

		private readonly bool isReadonly;

		public SeekableCryptoStream(EncryptionSettings encryptionSettings, string key, Stream stream)
		{
			if (!stream.CanRead)
				throw new ArgumentException("The Underlying stream for a SeekableCryptoStream must always be either read-only or read-write. Write only streams are not supported.");
			if (!stream.CanSeek)
				throw new ArgumentException("The Underlying stream for a SeekableCryptoStream must be seekable.");

			isReadonly = !stream.CanWrite;

			underlyingStream = new BlockReaderWriter(encryptionSettings, key, stream, Constants.DefaultIndexFileBlockSize);
			currentBlockSize = underlyingStream.Header.DecryptedBlockSize;
		}

		public override bool CanRead
		{
			get { return true; }
		}

		public override bool CanWrite
		{
			get { return !isReadonly; }
		}

		public override bool CanSeek
		{
			get { return true; }
		}

		public override int Read(byte[] buffer, int bufferOffset, int count)
		{
			//precaution, should never be true
			if (underlyingStream.Header.MagicNumber != EncryptedFile.WithTotalSizeMagicNumber &&
				underlyingStream.Header.MagicNumber != EncryptedFile.DefaultMagicNumber)
				throw new ApplicationException("Invalid magic number in the encrypted file. Cannot proceed with reading.");

			if (buffer == null)
				throw new ArgumentNullException("buffer");
			if (count < 0)
				throw new ArgumentOutOfRangeException("count");
			if (bufferOffset + count > buffer.LongLength)
				throw new ArgumentOutOfRangeException("bufferOffset");

			if (count == 0)
				return 0;

			lock (locker)
			{
				// If the stream is used for both reading and writing, make sure we're reading everything that was written
				WriteAnyUnwrittenData();

				if (Position >= underlyingStream.Footer.TotalLength &&
					underlyingStream.Header.MagicNumber == EncryptedFile.DefaultMagicNumber)
					return 0;

				if (Position >= underlyingStream.Header.TotalUnencryptedSize && 
					underlyingStream.Header.MagicNumber == EncryptedFile.WithTotalSizeMagicNumber)
					return 0;
	
				if (underlyingStream.Header.MagicNumber != EncryptedFile.WithTotalSizeMagicNumber && 
					underlyingStream.Header.MagicNumber != EncryptedFile.DefaultMagicNumber)
					throw new ApplicationException("Invalid magic number in the encrypted file. Cannot proceed with reading.");

				var startingBlock = underlyingStream.Header.GetBlockNumberFromLogicalPosition(Position);
				var blockOffset = underlyingStream.Header.GetBlockOffsetFromLogicalPosition(Position);

				if (currentReadingBlock == null || currentReadingBlock.BlockNumber != startingBlock)
				{
					currentReadingBlock = underlyingStream.ReadBlock(startingBlock);
				}

				int blockRead;
				if (underlyingStream.Header.MagicNumber == EncryptedFile.DefaultMagicNumber)
				{
					blockRead = (int) Math.Min(currentReadingBlock.TotalEncryptedStreamLength - Position, currentBlockSize - blockOffset);
				}
				else
				{
					blockRead = (int) Math.Min(underlyingStream.Header.TotalUnencryptedSize - Position, currentBlockSize - blockOffset);
				}
				
				var actualRead = Math.Min(count, blockRead);
				Array.Copy(currentReadingBlock.Data, blockOffset, buffer, bufferOffset, actualRead);
				// We use the fact that a stream doesn't have to read all data in one go to avoid a loop here.

				Position += actualRead;
				return actualRead;
			}
		}

		public override void Write(byte[] buffer, int bufferOffset, int count)
		{
			if (isReadonly)
				throw new InvalidOperationException("The current stream is read-only.");

			if (buffer == null)
				throw new ArgumentNullException("buffer");
			if (count < 0)
				throw new ArgumentOutOfRangeException("count");
			if (bufferOffset + count > buffer.LongLength)
				throw new ArgumentOutOfRangeException("bufferOffset");

			if (count == 0)
				return;

			lock (locker)
			{
				while (true)
				{
					long startingBlock = underlyingStream.Header.GetBlockNumberFromLogicalPosition(Position);
					long endingBlock = underlyingStream.Header.GetBlockNumberFromLogicalPosition(Position + count - 1);

					long blockOffset = underlyingStream.Header.GetBlockOffsetFromLogicalPosition(Position);

					if (currentWritingBlock == null || currentWritingBlock.BlockNumber != startingBlock)
						// If we're writing into a different block than the one that's currently in memory
					{
						WriteAnyUnwrittenData();

						if (blockOffset != 0 || count < currentBlockSize)
						{
							// Read the existing block from the underlying stream, as we're only changing part of it
							currentWritingBlock = underlyingStream.ReadBlock(startingBlock);
						    var lastBlock = underlyingStream.Header.GetBlockNumberFromLogicalPosition(Length);
						    if (lastBlock == startingBlock)
						    {
						        // this is the last block, it may not be a full one, so we need to make sure that it
                                // the actual size reflect that
						        currentWritingBlockUsage = underlyingStream.Header.GetBlockOffsetFromLogicalPosition(Length);
						    }
						    else
						    {
						        currentWritingBlockUsage = currentWritingBlock.Data.Length;// full size
						    }
						}
						else
						{
							// We're writing the entire block in one go
							currentWritingBlock = new EncryptedFile.Block
							{
								BlockNumber = startingBlock,
								Data = new byte[currentBlockSize],
								TotalEncryptedStreamLength = underlyingStream.Footer.TotalLength
							};
						    currentWritingBlockUsage = 0;
						}
					}
                  
                    if (startingBlock == endingBlock)
						// If the entire write is done to the same block
					{
                        currentWritingBlockUsage = Math.Max(currentWritingBlockUsage, blockOffset + count);
                        Array.Copy(buffer, bufferOffset, currentWritingBlock.Data, blockOffset, count);
						Position += count;
						break;
					}
                    var countInCurrentBlock = currentBlockSize - (int)blockOffset;
                    currentWritingBlockUsage = Math.Max(currentWritingBlockUsage, blockOffset + countInCurrentBlock);
					
					Array.Copy(buffer, bufferOffset, currentWritingBlock.Data, blockOffset, countInCurrentBlock);
					Position += countInCurrentBlock;

					// Write the next block from the same buffer
					bufferOffset += countInCurrentBlock;
					count -= countInCurrentBlock;
				}
			}
		}

		private void WriteAnyUnwrittenData()
		{
			if (currentWritingBlock != null)
			{
				underlyingStream.WriteBlock(currentWritingBlock, currentWritingBlockUsage);
				currentWritingBlock = null;
				currentReadingBlock = null;
			}
		}

		/// <summary>
		/// Note that this is the logical position in the stream, not the physical position on disk.
		/// </summary>
		public override long Position
		{
			get
			{
				lock (locker)
				{
					return currentPosition;
				}
			}
			set
			{
				lock (locker)
				{
					currentPosition = value;
					if (currentWritingBlock != null && currentWritingBlock.TotalEncryptedStreamLength < Position)
						currentWritingBlock.TotalEncryptedStreamLength = Position;
				}
			}
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			switch (origin)
			{
				case SeekOrigin.Begin:
					return Position = offset;

				case SeekOrigin.Current:
					return Position += offset;

				case SeekOrigin.End:
					throw new NotSupportedException("Seeking from the end of the stream is not supported.");

				default:
					throw new ArgumentException("Unknown SeekOrigin " + origin);
			}
		}

		public override void Flush()
		{
			lock (locker)
			{
				WriteAnyUnwrittenData();
				underlyingStream.Flush();
			}
		}

		protected override void Dispose(bool disposing)
		{
			Flush();
			underlyingStream.Dispose();
			base.Dispose(disposing);
		}

		public override long Length
		{
			get
			{
				var result = underlyingStream.Footer.TotalLength;

				// Even if we haven't flushed a block to the BlockReaderWriter, we need to count its size as written.
				if (currentWritingBlock != null)
					result = Math.Max(result, currentWritingBlock.TotalEncryptedStreamLength);

				return result;
			}
		}

		public override void SetLength(long value)
		{
			// usually this is done as an optimization, and we can't really 
			// support it here, so we ignore it.
		}

	}
}