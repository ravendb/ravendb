using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Index;
using Lucene.Net.Store;
using NLog;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Directory = Lucene.Net.Store.Directory;

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

		[Obsolete]
		// Note that this method is already obsoleted by Lucene, so disallowing it is okay.
		public override void RenameFile(string from, string to)
		{
			throw new NotSupportedException("Because encrypted files use their name as part of the encryption key, renaming is not supported.");
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
			return new FileInfo(Path.Combine(directory.FullName, name));
		}

		private void CreateDirectory()
		{
			if (!directory.Exists)
			{
				directory.Create();
				directory.Refresh();
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

		private class CodecIndexInput : IndexInput, IDisposable
		{
			private readonly Stream stream;
			private bool isStreamOwned;
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

			public override void Close()
			{
				GC.SuppressFinalize(this);
				if (isStreamOwned)
					stream.Close();
			}

			public override long GetFilePointer()
			{
				return position;
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

			public override void Seek(long newPosition)
			{
				lock (stream)
				{
					stream.Position = this.position = newPosition;
				}
			}

			public override object Clone()
			{
				return new CodecIndexInput(stream, position, false);
			}

			public void Dispose()
			{
				Close();
			}
		}

		private class CodecIndexOutput : IndexOutput
		{
			private readonly FileInfo file;
			private readonly Stream stream;

			public CodecIndexOutput(FileInfo file, Func<Stream, Stream> applyCodecs)
			{
				this.file = file;
				this.stream = applyCodecs(file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
			}

			~CodecIndexOutput()
			{
				var log = LogManager.GetCurrentClassLogger();
				try
				{
					log.Log(LogLevel.Error, "~CodecIndexOutput() " + file.FullName + "!");
					Close();
				}
				catch (Exception e)
				{
					// Can't throw exceptions from the finalizer thread
					log.LogException(LogLevel.Error, "Cannot dispose of CodecIndexOutput: " + e.Message, e);
				}
			}

			public override void Close()
			{
				GC.SuppressFinalize(this);
				stream.Close();
			}

			public override void Flush()
			{
				stream.Flush();
			}

			public override long GetFilePointer()
			{
				return stream.Position;
			}

			public override long Length()
			{
				return stream.Length;
			}

			public override void Seek(long pos)
			{
				stream.Seek(pos, SeekOrigin.Begin);
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
