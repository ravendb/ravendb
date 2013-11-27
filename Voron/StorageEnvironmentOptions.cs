using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron
{
	public interface IStorageQuotaOptions
	{
		long? MaxStorageSize { get; set; }

		long GetCurrentStorageSize();
	}

	public unsafe abstract class StorageEnvironmentOptions : IStorageQuotaOptions, IDisposable
	{
	    public EventHandler OnRecoveryError;

		public long MaxLogFileSize
		{
			get { return _maxLogFileSize; }
			set
			{
				if (value < _initialLogFileSize)
					InitialLogFileSize = value;
				_maxLogFileSize = value;
			}
		}

		public long InitialLogFileSize
		{
			get { return _initialLogFileSize; }
			set
			{
				if (value > MaxLogFileSize)
					MaxLogFileSize = value;
				_initialLogFileSize = value;
			}
		}

		protected long HeaderFilesSize = 2*sizeof (FileHeader);

		public bool OwnsPagers { get; set; }

		public bool ManualFlushing { get; set; }

		public bool IncrementalBackupEnabled { get; set; }

		public abstract IVirtualPager DataPager { get; }

		public long MaxNumberOfPagesInJournalBeforeFlush { get; set; }

		public int IdleFlushTimeout { get; set; }

		public long? MaxStorageSize { get; set; }

		public abstract IJournalWriter CreateJournalWriter(long journalNumber, long journalSize);

		public abstract long GetCurrentStorageSize();

		protected bool Disposed;
		private long _initialLogFileSize;
		private long _maxLogFileSize;

		protected StorageEnvironmentOptions()
		{
			MaxNumberOfPagesInJournalBeforeFlush = 1024; // 4 MB

			IdleFlushTimeout = 5000; // 5 seconds

			MaxLogFileSize = 64 * 1024 * 1024;

			InitialLogFileSize = 64 * 1024;

			OwnsPagers = true;
			IncrementalBackupEnabled = false;
			MaxStorageSize = null; // no quota
		}

		public static StorageEnvironmentOptions GetInMemory()
		{
			return new PureMemoryStorageEnvironmentOptions();
		}

		public static StorageEnvironmentOptions ForPath(string path)
		{
			return new DirectoryStorageEnvironmentOptions(path);
		}

		public IDisposable AllowManualFlushing()
		{
			var old = ManualFlushing;
			ManualFlushing = true;

			return new DisposableAction(() => ManualFlushing = old);
		}


		public class DirectoryStorageEnvironmentOptions : StorageEnvironmentOptions
		{
			private readonly string _basePath;
			private readonly Lazy<IVirtualPager> _dataPager;
			private IVirtualPager _scratchPager;

			private readonly ConcurrentDictionary<string, IJournalWriter> _journals =
				new ConcurrentDictionary<string, IJournalWriter>(StringComparer.OrdinalIgnoreCase);

			public DirectoryStorageEnvironmentOptions(string basePath)
			{
				_basePath = Path.GetFullPath(basePath);
				if (Directory.Exists(_basePath) == false)
				{
					Directory.CreateDirectory(_basePath);
				}
				_dataPager = new Lazy<IVirtualPager>(() => new MemoryMapPager(Path.Combine(_basePath, "db.voron"), this));
			}

			public override IVirtualPager DataPager
			{
				get
				{
					return _dataPager.Value;
				}
			}

			public string BasePath
			{
				get { return _basePath; }
			}

			public override IJournalWriter CreateJournalWriter(long journalNumber, long journalSize)
			{
				var name = JournalName(journalNumber);
				var path = Path.Combine(_basePath, name);
				var result = _journals.GetOrAdd(name, _ => new Win32FileJournalWriter(path, journalSize));

				if (result.Disposed)
				{
					var newWriter = new Win32FileJournalWriter(path, journalSize);
					if (_journals.TryUpdate(name, newWriter, result) == false)
						throw new InvalidOperationException("Could not update journal pager");
					result = newWriter;
				}

				return result;
			}

			public override long GetCurrentStorageSize()
			{
				long size = HeaderFilesSize + // headers
					_scratchPager.NumberOfAllocatedPages*AbstractPager.PageSize + // scratch file
					_journals.Values.Sum(x => x.NumberOfAllocatedPages*AbstractPager.PageSize); // journals

				if (_dataPager.IsValueCreated)
				{
					size += _dataPager.Value.NumberOfAllocatedPages*AbstractPager.PageSize;
				}

				//TODO arek - what if incremental backup is enabled, should we take into account unused journals too

				return size;
			}

			public override void Dispose()
			{
				if (Disposed)
					return;
				Disposed = true;
				if (_dataPager.IsValueCreated)
					_dataPager.Value.Dispose();
				foreach (var journal in _journals)
				{
					journal.Value.Dispose();
				}
			}

			public override bool TryDeleteJournal(long number)
			{
				var name = JournalName(number);

				IJournalWriter journal;
				if (_journals.TryRemove(name, out journal))
					journal.Dispose();

				var file = Path.Combine(_basePath, name);
				if (File.Exists(file) == false)
					return false;
				File.Delete(file);
				return true;
			}

			public unsafe override bool ReadHeader(string filename, FileHeader* header)
			{
				var path = Path.Combine(_basePath, filename);
				if (File.Exists(path) == false)
				{
					return false;
				}
				using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
				{
					var ptr = (byte*)header;
					int remaining = sizeof(FileHeader);
					while (remaining > 0)
					{
						int read;
						if (NativeFileMethods.ReadFile(fs.SafeFileHandle, ptr, remaining, out read, null) == false)
							throw new Win32Exception();
						ptr += read;
						remaining -= read;
					}
					return true;
				}
			}

			public override unsafe void WriteHeader(string filename, FileHeader* header)
			{
				var path = Path.Combine(_basePath, filename);
				using (var fs = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read))
				{
					var ptr = (byte*)header;
					int remaining = sizeof(FileHeader);
					while (remaining > 0)
					{
						int read;
						if (NativeFileMethods.WriteFile(fs.SafeFileHandle, ptr, remaining, out read, null) == false)
							throw new Win32Exception();
						ptr += read;
						remaining -= read;
					}
					NativeFileMethods.FlushFileBuffers(fs.SafeFileHandle);
				}
			}

			public override IVirtualPager CreateScratchPager()
			{
				_scratchPager = new MemoryMapPager(Path.Combine(_basePath, "scratch.buffers"), this, (NativeFileAttributes.DeleteOnClose | NativeFileAttributes.Temporary));
				return _scratchPager;
			}

			public override IVirtualPager OpenJournalPager(long journalNumber)
			{
				var name = JournalName(journalNumber);
				var path = Path.Combine(_basePath, name);
				if (File.Exists(path) == false)
					throw new InvalidOperationException("No such journal " + path);
				return new MemoryMapPager(path, this, access: NativeFileAccess.GenericRead);
			}
		}

		public class PureMemoryStorageEnvironmentOptions : StorageEnvironmentOptions
		{
			private readonly PureMemoryPager _dataPager;
			private PureMemoryPager _scratchPager;

			private Dictionary<string, IJournalWriter> _journals =
				new Dictionary<string, IJournalWriter>(StringComparer.OrdinalIgnoreCase);

			private Dictionary<string, IntPtr> _headers =
				new Dictionary<string, IntPtr>(StringComparer.OrdinalIgnoreCase);


			public PureMemoryStorageEnvironmentOptions()
			{
				_dataPager = new PureMemoryPager(this);
			}

			public override IVirtualPager DataPager
			{
				get { return _dataPager; }
			}

			public override IJournalWriter CreateJournalWriter(long journalNumber, long journalSize)
			{
				var name = JournalName(journalNumber);
				IJournalWriter value;
				if (_journals.TryGetValue(name, out value))
					return value;
				value = new PureMemoryJournalWriter(journalSize);
				_journals[name] = value;
				return value;
			}

			public override long GetCurrentStorageSize()
			{
				long size = HeaderFilesSize + // headers
				            _scratchPager.NumberOfAllocatedPages*AbstractPager.PageSize + // scratch file
				            _journals.Values.Sum(x => x.NumberOfAllocatedPages*AbstractPager.PageSize) + // journals
				            _dataPager.NumberOfAllocatedPages*AbstractPager.PageSize;

				//TODO arek - what if incremental backup is enabled, should we take into account unused journals too

				return size;
			}

			public override void Dispose()
			{
				if (Disposed)
					return;
				Disposed = true;
				_dataPager.Dispose();
				foreach (var virtualPager in _journals)
				{
					virtualPager.Value.Dispose();
				}

				foreach (var headerSpace in _headers)
				{
					Marshal.FreeHGlobal(headerSpace.Value);
				}
			}

			public override bool TryDeleteJournal(long number)
			{
				var name = JournalName(number);
				IJournalWriter value;
				if (_journals.TryGetValue(name, out value) == false)
					return false;
				_journals.Remove(name);
				value.Dispose();
				return true;
			}

			public unsafe override bool ReadHeader(string filename, FileHeader* header)
			{
				IntPtr ptr;
				if (_headers.TryGetValue(filename, out ptr) == false)
				{
					return false;
				}
				*header = *((FileHeader*)ptr);
				return true;
			}

			public override unsafe void WriteHeader(string filename, FileHeader* header)
			{
				IntPtr ptr;
				if (_headers.TryGetValue(filename, out ptr) == false)
				{
					ptr = Marshal.AllocHGlobal(sizeof(FileHeader));
					_headers[filename] = ptr;
				}
				NativeMethods.memcpy((byte*)ptr, (byte*)header, sizeof(FileHeader));
			}

			public override IVirtualPager CreateScratchPager()
			{
				_scratchPager = new PureMemoryPager(this);
				return _scratchPager;
			}

			public override IVirtualPager OpenJournalPager(long journalNumber)
			{
				var name = JournalName(journalNumber);
				IJournalWriter value;
				if (_journals.TryGetValue(name, out value))
					return value.CreatePager(this);
				throw new InvalidOperationException("No such journal " + journalNumber);
			}
		}

		public static string JournalName(long number)
		{
			return string.Format("{0:D19}.journal", number);
		}

		public abstract void Dispose();

		public abstract bool TryDeleteJournal(long number);

		public unsafe abstract bool ReadHeader(string filename, FileHeader* header);

		public unsafe abstract void WriteHeader(string filename, FileHeader* header);

		public abstract IVirtualPager CreateScratchPager();

		public abstract IVirtualPager OpenJournalPager(long journalNumber);
	}
}