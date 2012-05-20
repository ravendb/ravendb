using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Database.Indexing
{
	internal class LuceneCodecDirectory : FSDirectory
	{
		private readonly IEnumerable<AbstractIndexCodec> codecs;

		public LuceneCodecDirectory(string path, IEnumerable<AbstractIndexCodec> codecs) : base(new DirectoryInfo(path), null)
		{
			this.codecs = codecs ?? Enumerable.Empty<AbstractIndexCodec>();
		}

		public override IndexInput OpenInput(string name)
		{
			var file = GetFile(name);
			return new CodecIndexInput(file, s => ApplyReadCodecs(file.FullName, s));
		}

		public override IndexOutput CreateOutput(string name)
		{
			var file = GetFile(name);

			CreateDirectory();
			DeleteFile(file);

			return new CodecIndexOutput(file, s => ApplyWriteCodecs(file.FullName, s));
		}

		private Stream ApplyReadCodecs(string key, Stream stream)
		{
			foreach (var codec in codecs)
				stream = codec.Decode(key, stream);
			return stream;
		}

		private Stream ApplyWriteCodecs(string key, Stream stream)
		{
			foreach (var codec in codecs)
				stream = codec.Encode(key, stream);
			return stream;
		}

		private FileInfo GetFile(string name)
		{
			return new FileInfo(Path.Combine(directory.FullName, name));
		}

		private void CreateDirectory()
		{
			if (!directory.Exists)
				directory.Create();
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
			private readonly FileInfo file;
			private readonly Stream stream;

			public CodecIndexInput(FileInfo file, Func<Stream, Stream> applyCodecs)
			{
				this.file = file;
				this.stream = applyCodecs(file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
			}

			public override void Close()
			{
				stream.Close();
			}

			public override long GetFilePointer()
			{
				return stream.Position;
			}

			public override long Length()
			{
				return stream.Length;
			}

			public override byte ReadByte()
			{
				int value = stream.ReadByte();
				if (value == -1)
					throw new EndOfStreamException();
				return (byte)value;
			}

			public override void ReadBytes(byte[] b, int offset, int len)
			{
				int totalRead = 0;
				while (totalRead < len)
				{
					int read = stream.Read(b, offset + totalRead, len - totalRead);
					if (read == 0)
						throw new EndOfStreamException();
					totalRead += read;
				}
			}

			public override void Seek(long pos)
			{
				stream.Seek(pos, SeekOrigin.Begin);
			}
		}

		private class CodecIndexOutput : IndexOutput
		{
			private readonly FileInfo file;
			private readonly Stream stream;

			public CodecIndexOutput(FileInfo file, Func<Stream, Stream> applyCodecs)
			{
				this.file = file;
				this.stream = applyCodecs(file.Open(FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite));
			}

			public override void Close()
			{
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
