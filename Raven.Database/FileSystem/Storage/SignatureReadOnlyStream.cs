using System;
using System.IO;

using Raven.Database.Server.RavenFS.Storage.Esent;

namespace Raven.Database.Server.RavenFS.Storage
{
	public class SignatureReadOnlyStream : Stream
	{
		private readonly ITransactionalStorage storage;
		private readonly int id;
		private readonly int level;
		private long? length;

		public SignatureReadOnlyStream(ITransactionalStorage storage, int id, int level)
		{
			this.storage = storage;
			this.id = id;
			this.level = level;
		}

		public override void Flush()
		{
			throw new NotImplementedException();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			switch (origin)
			{
				case SeekOrigin.Begin:
					Position = offset;
					break;
				case SeekOrigin.End:
					Position = (long)(length + offset);
					break;
				case SeekOrigin.Current:
					Position += offset;
					break;
				default:
					throw new ArgumentOutOfRangeException("origin", origin, "Unknown origin");
			}

			return Position;
		}

		public override void SetLength(long value)
		{
			throw new NotImplementedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			var readBytes = 0;
			storage.Batch(
				accessor => accessor.GetSignatureStream(id, level, sigContent =>
				{
					sigContent.Position = Position;
					readBytes = sigContent.Read(buffer, offset, count);
					Position = sigContent.Position;
				}));

			return readBytes;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotImplementedException();
		}

		public override bool CanRead
		{
			get { return true; }
		}

		public override bool CanSeek
		{
			get { return true; }
		}

		public override bool CanWrite
		{
			get { return false; }
		}

		public override long Length
		{
			get
			{
				if (length == null)
				{
					storage.Batch(
						accessor => accessor.GetSignatureStream(id, level, sigContent => length = sigContent.Length));
				}

				return length.Value;
			}
		}

		public override long Position { get; set; }
	}
}