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
	///		- BlockSize: An Int32 for unecnrypted block size in bytes.
	///		- EncryptedBlockSize: An Int32 for ecnrypted block size in bytes.
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
		private long currentPosition;

		private readonly bool isReadonly;

		public SeekableCryptoStream(EncryptionSettings encryptionSettings, string key, Stream stream)
		{
			if (!stream.CanRead)
				throw new ArgumentException("The Underlying stream for a SeekableCryptoStream must always be either read-only or read-write. Write only streams are not supported.");
			if (!stream.CanSeek)
				throw new ArgumentException("The Underlying stream for a SeekableCryptoStream must be seekable.");

			isReadonly = !stream.CanWrite;

			this.underlyingStream = new BlockReaderWriter(encryptionSettings, key, stream, Constants.DefaultIndexFileBlockSize);
			this.currentBlockSize = underlyingStream.Header.DecryptedBlockSize;
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

				if (Position >= underlyingStream.Footer.TotalLength)
					return 0;

				long startingBlock = underlyingStream.Header.GetBlockNumberFromLogicalPosition(Position);
				long blockOffset = underlyingStream.Header.GetBlockOffsetFromLogicalPosition(Position);

				if (currentReadingBlock == null || currentReadingBlock.BlockNumber != startingBlock)
				{
					currentReadingBlock = underlyingStream.ReadBlock(startingBlock);
				}

				int blockRead = (int)Math.Min(currentReadingBlock.TotalStreamLength - Position, currentBlockSize - bufferOffset);
				int actualRead = Math.Min(count, blockRead);
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
			writeStart:

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
					}
					else
					{
						// We're writing the entire block in one go
						currentWritingBlock = new EncryptedFile.Block
						{
							BlockNumber = startingBlock,
							Data = new byte[currentBlockSize],
							TotalStreamLength = underlyingStream.Footer.TotalLength
						};
					}
				}

				if (startingBlock == endingBlock)
				// If the entire write is done to the same block
				{
					Array.Copy(buffer, bufferOffset, currentWritingBlock.Data, blockOffset, count);
					Position += count;
				}
				else
				{
					var countInCurrentBlock = currentBlockSize - bufferOffset;
					Array.Copy(buffer, bufferOffset, currentWritingBlock.Data, blockOffset, countInCurrentBlock);
					Position += countInCurrentBlock;

					// Write the next block from the same buffer
					bufferOffset += countInCurrentBlock;
					count -= countInCurrentBlock;
					goto writeStart;
				}
			}
		}

		private void WriteAnyUnwrittenData()
		{
			if (currentWritingBlock != null)
			{
				underlyingStream.WriteBlock(currentWritingBlock);
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
					if (currentWritingBlock != null && currentWritingBlock.TotalStreamLength < Position)
						currentWritingBlock.TotalStreamLength = Position;
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
					result = Math.Max(result, currentWritingBlock.TotalStreamLength);

				return result;
			}
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}
	}
}