using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Lucene.Net.Store;
using Microsoft.Win32.SafeHandles;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Sparrow;
using Voron.Platform.Win32;

namespace Raven.Database.Indexing
{
	public class LuceneCodecDirectory : FSDirectory
	{
		private readonly List<AbstractIndexCodec> codecs;

		public LuceneCodecDirectory(string path, IEnumerable<AbstractIndexCodec> codecs)
			: base(new DirectoryInfo(path), null)
		{
			this.codecs = (codecs ?? Enumerable.Empty<AbstractIndexCodec>()).ToList();
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
		    return GetFile(name).Length;
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
					IOExtensions.DeleteFile(file.FullName);
				}
				catch (Exception e)
				{
					throw new IOException("Cannot overwrite " + file, e);
				}
			}
		}
		private unsafe class CodecIndexInput : IndexInput
		{
			private readonly FileInfo file;
			private readonly Func<Stream, Stream> applyCodecs;
		    private Stream stream;
		    private bool isOriginal = true;
		    private readonly MemoryMappedFile _mmf;
		    private readonly byte* _basePtr;
            private readonly CancellationTokenSource _cts = new CancellationTokenSource();

		    public CodecIndexInput(FileInfo file, Func<Stream, Stream> applyCodecs)
			{
				this.file = file;
				this.applyCodecs = applyCodecs;

		        if (file.Exists == false)
		            throw new FileNotFoundException(file.FullName);

			    _mmf = MemoryMappedFile.CreateFromFile(file.FullName,FileMode.Open);
		        _basePtr = Win32MemoryMapNativeMethods.MapViewOfFileEx(_mmf.SafeMemoryMappedFileHandle.DangerousGetHandle(),
		            Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read,
		            0, 0, UIntPtr.Zero, null);
		        if (_basePtr == null)
		            throw new Win32Exception();

		        stream = applyCodecs(new UnmanagedMemoryStream(_basePtr, file.Length, file.Length, FileAccess.Read));
			}

		    public override object Clone()
		    {
                if (_cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");

                var clone = (CodecIndexInput) base.Clone();
                GC.SuppressFinalize(clone);
		        clone.isOriginal = false;
                clone.stream = applyCodecs(new UnmanagedMemoryStream(_basePtr, file.Length, file.Length, FileAccess.Read));
                return clone;
		    }

		    public override byte ReadByte()
		    {
                if (_cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");
                var readByte = stream.ReadByte();
		        if (readByte == -1)
		            throw new EndOfStreamException();
		        return (byte)readByte;
		    }

		    public override void ReadBytes(byte[] b, int offset, int len)
		    {
                if (_cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");
                stream.Read(b, offset, len);
		    }

		    protected override void Dispose(bool disposing)
		    {
                stream.Dispose();

                if (isOriginal == false)
                    return;

		        GC.SuppressFinalize(this);
		        _cts.Cancel();
                Win32MemoryMapNativeMethods.UnmapViewOfFile(_basePtr);
                _mmf.Dispose();
		    }

            ~CodecIndexInput()
            {
                try
                {
                    Dispose(false);
                }
                catch (Exception)
                {
                    // nothing can be done here
                }
            }

		    public override void Seek(long pos)
		    {
                if(_cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");
		        stream.Seek(pos, SeekOrigin.Begin);
		    }

		    public override long Length()
		    {

		        return file.Length;
		    }

		    public override long FilePointer
		    {
		        get
		        {
                    if (_cts.IsCancellationRequested)
                        throw new ObjectDisposedException("CodecIndexInput");
                    return stream.Position;
		        }
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
			    if (stream != null)
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
