using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron
{
	public abstract class StorageEnvironmentOptions : IDisposable
	{
		public event EventHandler<RecoveryErrorEventArgs> OnRecoveryError;

		public void InvokeRecoveryError(object sender, string message, Exception e)
		{
			var handler = OnRecoveryError;
			if (handler == null)
			{
				throw new InvalidDataException(message + Environment.NewLine +
					 "An exception has been thrown because there isn't a listener to the OnRecoveryError event on the storage options.", e);
			}

			handler(this, new RecoveryErrorEventArgs(message, e));
		}

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

		public bool OwnsPagers { get; set; }

		public bool ManualFlushing { get; set; }

		public bool IncrementalBackupEnabled { get; set; }

		public abstract IVirtualPager DataPager { get; }

		public long MaxNumberOfPagesInJournalBeforeFlush { get; set; }

		public int IdleFlushTimeout { get; set; }

		public long? MaxStorageSize { get; set; }

		public abstract IJournalWriter CreateJournalWriter(long journalNumber, long journalSize);

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

			private readonly ConcurrentDictionary<string, Lazy<IJournalWriter>> _journals =
				new ConcurrentDictionary<string, Lazy<IJournalWriter>>(StringComparer.OrdinalIgnoreCase);

			public DirectoryStorageEnvironmentOptions(string basePath)
			{
				_basePath = Path.GetFullPath(basePath);
				
				if (Directory.Exists(_basePath) == false)
				{
					Directory.CreateDirectory(_basePath);
				}
				_dataPager = new Lazy<IVirtualPager>(() => new Win32MemoryMapPager(Path.Combine(_basePath, Constants.DatabaseFilename)));
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
				var result = _journals.GetOrAdd(name, _ => new Lazy<IJournalWriter>(() => new Win32FileJournalWriter(path, journalSize)));

				if (result.Value.Disposed)
				{
					var newWriter = new Lazy<IJournalWriter>(() =>  new Win32FileJournalWriter(path, journalSize));
					if (_journals.TryUpdate(name, newWriter, result) == false)
						throw new InvalidOperationException("Could not update journal pager");
					result = newWriter;
				}

				return result.Value;
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
					if (journal.Value.IsValueCreated)
						journal.Value.Value.Dispose();
				}
			}

			public override bool TryDeleteJournal(long number)
			{
				var name = JournalName(number);

				Lazy<IJournalWriter> lazy;
				if (_journals.TryRemove(name, out lazy) && lazy.IsValueCreated)
					lazy.Value.Dispose();

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

			public override IVirtualPager CreateScratchPager(string name)
			{
			    var scratchFile = Path.Combine(_basePath, name);
			    if (File.Exists(scratchFile)) 
                    File.Delete(scratchFile);

                return new Win32MemoryMapPager(scratchFile, (NativeFileAttributes.DeleteOnClose | NativeFileAttributes.Temporary));
			}

			public override IVirtualPager OpenJournalPager(long journalNumber)
			{
				var name = JournalName(journalNumber);
				var path = Path.Combine(_basePath, name);
				if (File.Exists(path) == false)
					throw new InvalidOperationException("No such journal " + path);
				return new Win32MemoryMapPager(path, access: NativeFileAccess.GenericRead);
			}
		}

		public class PureMemoryStorageEnvironmentOptions : StorageEnvironmentOptions
		{
			private readonly Win32PureMemoryPager _dataPager;

			private Dictionary<string, IJournalWriter> _logs =
				new Dictionary<string, IJournalWriter>(StringComparer.OrdinalIgnoreCase);

			private Dictionary<string, IntPtr> _headers =
				new Dictionary<string, IntPtr>(StringComparer.OrdinalIgnoreCase);


			public PureMemoryStorageEnvironmentOptions()
			{
				_dataPager = new Win32PureMemoryPager();
			}

			public override IVirtualPager DataPager
			{
				get { return _dataPager; }
			}

			public override IJournalWriter CreateJournalWriter(long journalNumber, long journalSize)
			{
				var name = JournalName(journalNumber);
				IJournalWriter value;
				if (_logs.TryGetValue(name, out value))
					return value;
				value = new PureMemoryJournalWriter(journalSize);
				_logs[name] = value;
				return value;
			}

			public override void Dispose()
			{
				if (Disposed)
					return;
				Disposed = true;
				_dataPager.Dispose();
				foreach (var virtualPager in _logs)
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
				if (_logs.TryGetValue(name, out value) == false)
					return false;
				_logs.Remove(name);
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

			public override IVirtualPager CreateScratchPager(string name)
			{
				return new Win32PureMemoryPager();
			}

			public override IVirtualPager OpenJournalPager(long journalNumber)
			{
				var name = JournalName(journalNumber);
				IJournalWriter value;
				if (_logs.TryGetValue(name, out value))
					return value.CreatePager();
				throw new InvalidOperationException("No such journal " + journalNumber);
			}
		}

		public static string JournalName(long number)
		{
			return string.Format("{0:D19}.journal", number);
		}

		public static string JournalRecoveryName(long number)
		{
			return string.Format("{0:D19}.recovery", number);
		}

		public abstract void Dispose();

		public abstract bool TryDeleteJournal(long number);

		public unsafe abstract bool ReadHeader(string filename, FileHeader* header);

		public unsafe abstract void WriteHeader(string filename, FileHeader* header);

		public abstract IVirtualPager CreateScratchPager(string name);

		public abstract IVirtualPager OpenJournalPager(long journalNumber);
	}
}
