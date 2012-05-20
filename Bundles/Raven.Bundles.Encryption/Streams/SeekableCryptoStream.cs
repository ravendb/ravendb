using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
	///		- BlockSize: An Int32 for block size in bytes. This should be the same for an encrypted and decrypted block.
	///	Block:
	///		- BlockIV: byte[], Length == Header.IV
	///		- Data: byte[], Length == Header.BlockSize
	/// </summary>
	internal class SeekableCryptoStream : Stream
	{
		private const int DefaultBlockSize = 1024;
		private int CurrentBlockSize = 1024;

		private readonly BlockReaderWriter underlyingStream;
		private readonly object locker;
		private EncryptedFile.Block currentReadingBlock;
		private EncryptedFile.Block currentWritingBlock;
		private long currentPosition;

		public SeekableCryptoStream(string key, Stream stream)
		{
			this.underlyingStream = new BlockReaderWriter(key, stream, DefaultBlockSize);
		}

		public override bool CanRead
		{
			get { return true; }
		}

		public override bool CanWrite
		{
			get { return true; }
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
			if (bufferOffset + count < buffer.LongLength)
				throw new ArgumentOutOfRangeException("bufferOffset");

			if (count == 0)
				return 0;

			lock (locker)
			{
				// If the stream is used for both reading and writing, make sure we're reading everything that was written
				WriteAnyUnwrittenData();

				long startingBlock = underlyingStream.Header.GetBlockNumberFromLogicalPosition(Position);
				long blockOffset = underlyingStream.Header.GetBlockOffsetFromLogicalPosition(Position);

				if (currentReadingBlock == null || currentReadingBlock.BlockNumber != startingBlock)
				{
					currentReadingBlock = underlyingStream.ReadBlock(startingBlock);
				}

				int actualRead = Math.Min(count, CurrentBlockSize - bufferOffset);
				Array.Copy(currentReadingBlock.Data, blockOffset, buffer, bufferOffset, actualRead);
				// We use the fact that a stream doesn't have to read all data in one go to avoid a loop here.
				return actualRead;
			}
		}

		public override void Write(byte[] buffer, int bufferOffset, int count)
		{
			if (buffer == null)
				throw new ArgumentNullException("buffer");
			if (count < 0)
				throw new ArgumentOutOfRangeException("count");
			if (bufferOffset + count < buffer.LongLength)
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

					if (blockOffset != 0 || count < CurrentBlockSize)
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
							Data = new byte[CurrentBlockSize]
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
					var countInCurrentBlock = CurrentBlockSize - bufferOffset;
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
			get { throw new NotSupportedException(); }
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}
	}
}