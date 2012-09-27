using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Store;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Plugins;

namespace Raven.Database.Indexing
{
	internal class LuceneCodecDirectory : FSDirectory
	{
		private readonly IEnumerable<AbstractIndexCodec> codecs;

		public LuceneCodecDirectory(string path, IEnumerable<AbstractIndexCodec> codecs)
			: base(new DirectoryInfo(path), null)
		{
			this.codecs = codecs ?? Enumerable.Empty<AbstractIndexCodec>();
		}

		public override IndexInput OpenInput(string name)
		{
			return OpenInputInner(name);
		}

		public override IndexInput OpenInput(string name, int bufferSize)
		{
			return OpenInputInner(name);
		}

		private CodecIndexInput OpenInputInner(string name)
		{
			var file = GetFile(name);
			return new CodecIndexInput(file, s => ApplyReadCodecs(file.Name, s));
		}

		public override IndexOutput CreateOutput(string name)
		{
			var file = GetFile(name);

			CreateDirectory();
			DeleteFile(file);

			return new CodecIndexOutput(file, s => ApplyWriteCodecs(file.Name, s));
		}

		public override long FileLength(string name)
		{
			using (var input = OpenInputInner(name))
				return input.Length();
		}

		private Stream ApplyReadCodecs(string key, Stream stream)
		{
			try
			{
				foreach (var codec in codecs)
					stream = codec.Decode(key, stream);
				return stream;
			}
			catch
			{
				stream.Dispose();
				throw;
			}
		}

		private Stream ApplyWriteCodecs(string key, Stream stream)
		{
			try
			{
				foreach (var codec in codecs)
					stream = codec.Encode(key, stream);
				return stream;
			}
			catch
			{
				stream.Dispose();
				throw;
			}
		}

		private FileInfo GetFile(string name)
		{
			return new FileInfo(Path.Combine(Directory.FullName, name));
		}

		private void CreateDirectory()
		{
			if (!Directory.Exists)
			{
				Directory.Create();
				Directory.Refresh();
			}
		}

		private void DeleteFile(FileInfo file)
		{
			if (file.Exists)
			{
				try
				{
					file.Delete();
				}
				catch (Exception e)
				{
					throw new IOException("Cannot overwrite " + file, e);
				}
			}
		}

		private class CodecIndexInput : IndexInput
		{
			private readonly Stream stream;
			private readonly bool isStreamOwned;
			private long position;

			public CodecIndexInput(FileInfo file, Func<Stream, Stream> applyCodecs)
				: this(
					stream: applyCodecs(file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite)),
					position: 0,
					isStreamOwned: true
				)
			{ }

			private CodecIndexInput(Stream stream, long position, bool isStreamOwned)
			{
				this.stream = stream;
				this.position = position;
				this.isStreamOwned = isStreamOwned;

				if (!isStreamOwned)
					GC.SuppressFinalize(this);
			}

			public override long Length()
			{
				return stream.Length;
			}

			public override byte ReadByte()
			{
				// The lock must be on the stream, because it is this object which is shared between the different clones.
				lock (stream)
				{
					stream.Position = position;

					int value = stream.ReadByte();
					if (value == -1)
						throw new EndOfStreamException();

					position = stream.Position;
					return (byte)value;
				}
			}

			public override void ReadBytes(byte[] b, int offset, int len)
			{
				lock (stream)
				{
					stream.Position = position;

					stream.ReadEntireBlock(b, offset, len);

					position = stream.Position;
				}
			}

			protected override void Dispose(bool disposing)
			{
				GC.SuppressFinalize(this);
				if (isStreamOwned)
					stream.Close();
			}

			public override void Seek(long newPosition)
			{
				lock (stream)
				{
					stream.Position = position = newPosition;
				}
			}

			public override object Clone()
			{
				return new CodecIndexInput(stream, position, false);
			}

			public override long FilePointer
			{
				get { return position; }
			}
		}

		private class CodecIndexOutput : IndexOutput
		{
			static readonly ILog log = LogManager.GetCurrentClassLogger();

			private readonly FileInfo file;
			private readonly Stream stream;

			public CodecIndexOutput(FileInfo file, Func<Stream, Stream> applyCodecs)
			{
				this.file = file;
				stream = applyCodecs(file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
			}

			~CodecIndexOutput()
			{
				try
				{
					log.Error("~CodecIndexOutput() " + file.FullName + "!");
					Dispose(false);
				}
				catch (Exception e)
				{
					// Can't throw exceptions from the finalizer thread
					log.ErrorException("Cannot dispose of CodecIndexOutput: " + e.Message, e);
				}
			}

			public override void Flush()
			{
				stream.Flush();
			}

			protected override void Dispose(bool disposing)
			{
				GC.SuppressFinalize(this);
				stream.Close();
			}

			public override void Seek(long pos)
			{
				stream.Seek(pos, SeekOrigin.Begin);
			}

			public override long FilePointer
			{
				get { return stream.Position; }
			}
			public override long Length
			{
				get { return stream.Length; }
			}

			public override void WriteByte(byte b)
			{
				stream.WriteByte(b);
			}

			public override void WriteBytes(byte[] bytes, int offset, int length)
			{
				stream.Write(bytes, offset, length);
			}
		}
	}
}
