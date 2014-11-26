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
	public class LuceneCodecDirectory : FSDirectory
	{
		private readonly IEnumerable<AbstractIndexCodec> codecs;

		public LuceneCodecDirectory(string path, IEnumerable<AbstractIndexCodec> codecs)
			: base(new DirectoryInfo(path), null)
		{
			this.codecs = codecs ?? Enumerable.Empty<AbstractIndexCodec>();
		}

		public override IndexInput OpenInput(string name, int bufferSize)
		{
			return OpenInputInner(name, bufferSize);
		}

		private CodecIndexInput OpenInputInner(string name, int bufferSize)
		{
			var file = GetFile(name);
			return new CodecIndexInput(file, s => ApplyReadCodecs(file.Name, s), bufferSize);
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
			using (var input = OpenInputInner(name, bufferSize: BufferedIndexInput.BUFFER_SIZE))
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

		private class CodecIndexInput : BufferedIndexInput
		{
			private FileInfo file;
			private Func<Stream, Stream> applyCodecs;
			private Stream stream;

			public CodecIndexInput(FileInfo file, Func<Stream, Stream> applyCodecs, int bufferSize)
				: base(bufferSize)
			{
				this.file = file;
				this.applyCodecs = applyCodecs;
				this.stream = applyCodecs(file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
			}

			public override long Length()
			{
				return stream.Length;
			}

			protected override void Dispose(bool disposing)
			{
				stream.Close();
			}

			public override void ReadInternal(byte[] b, int offset, int length)
			{
				stream.Position = FilePointer;

				stream.ReadEntireBlock(b, offset, length);
			}

			public override void SeekInternal(long pos)
			{
			}

			public override object Clone()
			{
				var codecIndexInput = (CodecIndexInput) base.Clone();
				codecIndexInput.file = file;
				codecIndexInput.applyCodecs = applyCodecs;
				codecIndexInput.stream = applyCodecs(file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
				return codecIndexInput;
			}
		}

		private class CodecIndexOutput : BufferedIndexOutput
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

			public override void FlushBuffer(byte[] b, int offset, int len)
			{
				stream.Write(b, offset, len);
				stream.Flush();
			}

			protected override void Dispose(bool disposing)
			{
				base.Dispose(disposing);
				stream.Close();
				GC.SuppressFinalize(this);
			}

			public override void Seek(long pos)
			{
				base.Seek(pos);
				stream.Seek(pos, SeekOrigin.Begin);
			}

			public override void SetLength(long length)
			{
				stream.SetLength(length);
			}

			public override long Length
			{
				get { return stream.Length; }
			}
		}
	}
}
