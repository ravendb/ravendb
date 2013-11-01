// -----------------------------------------------------------------------
//  <copyright file="WriteAheadJournal.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron.Impl.Journal
{
	public unsafe class WriteAheadJournal : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly IVirtualPager _dataPager;
		internal readonly Func<long, string> LogName = number => string.Format("{0:D19}.journal", number);

		private long _currentJournalFileSize;
		private DateTime _lastFile;
		private readonly IList<JournalFile> _splitJournalFiles;

		private long _logIndex = -1;
		private FileHeader* _fileHeader;
		private IntPtr _inMemoryHeader;
		private long _dataFlushCounter;

		internal ImmutableList<JournalFile> Files = ImmutableList<JournalFile>.Empty;
		internal JournalFile CurrentFile;

		private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

		public WriteAheadJournal(StorageEnvironment env)
		{
			_env = env;
			_dataPager = _env.Options.DataPager;
			_fileHeader = GetEmptyFileHeader();
			_splitJournalFiles = new List<JournalFile>();
			_currentJournalFileSize = env.Options.InitialLogFileSize;
		}

		private JournalFile NextFile(Transaction tx, int numberOfPages = 1)
		{
			_logIndex++;

			var logPager = _env.Options.CreateJournalPager(LogName(_logIndex));

			var now = DateTime.UtcNow;
			if ((now - _lastFile).TotalSeconds < 90)
			{
				_currentJournalFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentJournalFileSize * 2);
			}
			var actualLogSize = _currentJournalFileSize;
			var minRequiredsize = (numberOfPages + 1) * logPager.PageSize; // number of pages + tx header page
			if (_currentJournalFileSize < minRequiredsize)
			{
				actualLogSize = minRequiredsize;
			}
			_lastFile = now;

			logPager.AllocateMorePages(null, actualLogSize);

			var log = new JournalFile(logPager, _logIndex);
			log.AddRef(); // one reference added by a creator - write ahead log
			tx.SetLogReference(log); // and the next one for the current transaction


			// protect against readers trying to modify anything here
			_locker.EnterWriteLock();
			try
			{
				Files = Files.Add(log);

				UpdateLogInfo();
				WriteFileHeader();
			}
			finally
			{
				_locker.ExitWriteLock();
			}

			_dataPager.Sync(); // we have to flush the new log information to disk, may be expensive, so we do it outside the lock

			return log;
		}

		public void RecoverDatabase(FileHeader* fileHeader, out TransactionHeader* lastTxHeader)
		{
			// note, we don't need to do any concurrency here, happens as a single threaded
			// fashion on db startup

			_fileHeader = CopyFileHeader(fileHeader);
			var logInfo = _fileHeader->LogInfo;

			lastTxHeader = null;

			if (logInfo.LogFilesCount == 0)
			{
				return;
			}

			for (var logNumber = logInfo.RecentLog - logInfo.LogFilesCount + 1; logNumber <= logInfo.RecentLog; logNumber++)
			{
				var pager = _env.Options.CreateJournalPager(LogName(logNumber));
				RecoverCurrentJournalSize(pager);
				var log = new JournalFile(pager, logNumber);
				log.AddRef(); // creator reference - write ahead log
				Files = Files.Add(log);
			}

			foreach (var logItem in Files)
			{
				long startRead = 0;

				if (logItem.Number == logInfo.LastSyncedLog)
					startRead = logInfo.LastSyncedLogPage + 1;

				lastTxHeader = logItem.RecoverAndValidate(startRead, lastTxHeader);
			}

			_logIndex = logInfo.RecentLog;
			_dataFlushCounter = logInfo.DataFlushCounter + 1;

			var lastFile = Files.Last();
			if (lastFile.AvailablePages >= 2) // it must have at least one page for the next transaction header and one page for data
				CurrentFile = lastFile;
		}

		private void RecoverCurrentJournalSize(IVirtualPager pager)
		{
			var journalSize = pager.NumberOfAllocatedPages * pager.PageSize;
			if (journalSize >= _env.Options.MaxLogFileSize) // can't set for more than the max log file size
				return;

			// we want to ignore big single value log files (a log file with a single 2 MB value is considered rare), so we don't
			// want to jump the size just for a single value, so we ignore values that aren't multiples of the 
			// initial size (our base)
			if (journalSize%_env.Options.InitialLogFileSize != 0)
				return;

			_currentJournalFileSize = journalSize;
		}

		public void UpdateLogInfo()
		{
			_fileHeader->LogInfo.RecentLog = Files.Count > 0 ? _logIndex : -1;
			_fileHeader->LogInfo.LogFilesCount = Files.Count;
			_fileHeader->LogInfo.DataFlushCounter = _dataFlushCounter;

			_fileHeader->BackupInfo.LastCreatedJournal = _logIndex;
		}

		internal void WriteFileHeader(long? pageToWriteHeader = null)
		{
			var fileHeaderPage = _dataPager.TempPage;

			long page = pageToWriteHeader ?? _dataFlushCounter & 1;

			var header = (FileHeader*)(fileHeaderPage.Base);
			header->MagicMarker = Constants.MagicMarker;
			header->Version = Constants.CurrentVersion;
			header->TransactionId = _fileHeader->TransactionId;
			header->LastPageNumber = _fileHeader->LastPageNumber;

			header->Root = _fileHeader->Root;
			header->FreeSpace = _fileHeader->FreeSpace;
			header->LogInfo = _fileHeader->LogInfo;

			_dataPager.Write(fileHeaderPage, page);
		}

		public void TransactionBegin(Transaction tx)
		{
			if (CurrentFile == null)
				CurrentFile = NextFile(tx);

			if (_splitJournalFiles.Count > 0) // last split transaction was not committed
			{
				Debug.Assert(_splitJournalFiles.All(x => x.LastTransactionCommitted == false));
				CurrentFile = _splitJournalFiles[0];
				_splitJournalFiles.Clear();
			}

			CurrentFile.TransactionBegin(tx);
		}

		public void TransactionCommit(Transaction tx)
		{
			if (_splitJournalFiles.Count > 0)
			{
				foreach (var journalFile in _splitJournalFiles)
					journalFile.TransactionCommit(tx);

				_splitJournalFiles.Clear();
			}

			CurrentFile.TransactionCommit(tx);

			if (CurrentFile.AvailablePages < 2) // it must have at least one page for the next transaction header and one page for data
			{
				CurrentFile = null; // it will force new log file creation when next transaction will start
			}
		}

		public Page ReadPage(Transaction tx, long pageNumber)
		{
			// read transactions have to read from log snapshots
			if (tx.Flags == TransactionFlags.Read)
			{
				// read log snapshots from the back to get the most recent version of a page
				for (var i = tx.LogSnapshots.Count - 1; i >= 0; i--)
				{
					var page = tx.LogSnapshots[i].ReadPage(pageNumber);
					if (page != null)
						return page;
				}

				return null;
			}

			// write transactions can read directly from logs
			for (var i = Files.Count - 1; i >= 0; i--)
			{
				var page = Files[i].ReadPage(tx, pageNumber);
				if (page != null)
					return page;
			}

			return null;
		}

		public Page Allocate(Transaction tx, long startPage, int numberOfPages)
		{
			if (CurrentFile.AvailablePages < numberOfPages)
			{
				// here we need to mark that transaction is split in both log files
				// it will have the following transaction markers in the headers
				// log_1: [Start|Split|Commit] log_2: [Split|Commit]

				CurrentFile.TransactionSplit(tx);
				_splitJournalFiles.Add(CurrentFile);

				CurrentFile = NextFile(tx, numberOfPages);

				CurrentFile.TransactionSplit(tx);
			}

			return CurrentFile.Allocate(startPage, numberOfPages);
		}


		public void Dispose()
		{
			if (_inMemoryHeader != IntPtr.Zero)
			{
				Marshal.FreeHGlobal(_inMemoryHeader);
				_inMemoryHeader = IntPtr.Zero;
			}

			if (_env.Options.OwnsPagers)
			{
				foreach (var logFile in Files)
				{
					logFile.Dispose();
				}
			}
			else
			{
				foreach (var logFile in Files)
				{
					GC.SuppressFinalize(logFile);
				}

			}

			Files.Clear();
		}

		private FileHeader* GetEmptyFileHeader()
		{
			if (_inMemoryHeader == IntPtr.Zero)
				_inMemoryHeader = Marshal.AllocHGlobal(_dataPager.PageSize);

			NativeMethods.memset((byte*)_inMemoryHeader.ToPointer(), 0, _dataPager.PageSize);

			var header = (FileHeader*)_inMemoryHeader;

			header->MagicMarker = Constants.MagicMarker;
			header->Version = Constants.CurrentVersion;
			header->TransactionId = 0;
			header->LastPageNumber = 1;
			header->FreeSpace.RootPageNumber = -1;
			header->Root.RootPageNumber = -1;
			header->LogInfo.DataFlushCounter = -1;
			header->LogInfo.RecentLog = -1;
			header->LogInfo.LogFilesCount = 0;
			header->LogInfo.LastSyncedLog = -1;
			header->LogInfo.LastSyncedLogPage = -1;

			return header;
		}

		private FileHeader* CopyFileHeader(FileHeader* fileHeader)
		{
			Debug.Assert(_inMemoryHeader != IntPtr.Zero);

			NativeMethods.memcpy((byte*)_inMemoryHeader, (byte*)fileHeader, sizeof(FileHeader));

			return (FileHeader*)_inMemoryHeader;
		}

		public List<LogSnapshot> GetSnapshots()
		{
			return Files.Select(x => x.GetSnapshot()).ToList();
		}


		public bool HasTransactionsToFlush()
		{
			_locker.EnterReadLock();
			try
			{
				var currentFile = CurrentFile;
				if (currentFile == null)
					return false;

				using (var tx = _env.NewTransaction(TransactionFlags.Read))
				{
					var lastSyncedLog = _fileHeader->LogInfo.LastSyncedLog;
					var lastSyncedLogPage = _fileHeader->LogInfo.LastSyncedLogPage;

					if (lastSyncedLog == currentFile.Number &&
						lastSyncedLogPage == currentFile.WritePagePosition - 1)
						return false;

					tx.Commit();
					return true;
				}
			}
			finally
			{
				_locker.ExitReadLock();
			}
		}

		public class JournalApplicator
		{
			private readonly WriteAheadJournal _waj;
			private readonly long _oldestActiveTransaction;
			private long _lastSyncedLog;
			private long _lastSyncedPage;
			private TransactionHeader* _lastTransactionHeader;
			private ImmutableList<JournalFile> _jrnls;

			public JournalApplicator(WriteAheadJournal waj, long oldestActiveTransaction)
			{
				_waj = waj;
				_oldestActiveTransaction = oldestActiveTransaction;
			}


			public void ApplyLogsToDataFile()
			{
				using (var tx = _waj._env.NewTransaction(TransactionFlags.Read))
				{
					_waj._locker.EnterReadLock();
					try
					{
						_jrnls = _waj.Files;
						_lastSyncedLog = _waj._fileHeader->LogInfo.LastSyncedLog;
						_lastSyncedPage = _waj._fileHeader->LogInfo.LastSyncedLogPage;
						Debug.Assert(_jrnls.First().Number >= _lastSyncedLog);
					}
					finally
					{
						_waj._locker.ExitReadLock();
					}

					if (_jrnls.Count == 0)
						return;

					var pagesToWrite = ReadTransactionsToFlush(_oldestActiveTransaction, _jrnls);

					if (pagesToWrite.Count == 0)
						return;

					Debug.Assert(_lastTransactionHeader != null);

					var sortedPages = pagesToWrite.OrderBy(x => x.Key)
												  .Select(x => x.Value.ReadPage(null, x.Key))
												  .ToList();

					var last = sortedPages.Last();

					var lastPage = last.IsOverflow == false ? 1 :
						_waj._env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

					_waj._dataPager.EnsureContinuous(null, last.PageNumber, lastPage);

					foreach (var page in sortedPages)
					{
						_waj._dataPager.Write(page);
					}

					var journalFiles = _jrnls.RemoveAll(x => x.Number >= _lastSyncedLog);

					// we want to hold the write lock for as little time as possible, therefor
					// what we do is take the lock, write the appropriate metadata, then exit the lock
					// the actual fsync (expensive) is happening outside the lock, and when can then 
					// decide to clear the old logs if we want to.
					_waj._locker.EnterWriteLock();
					try
					{
						UpdateFileHeaderAfterDataFileSync(tx);

						_waj.Files = _waj.Files.RemoveAll(x => x.Number < _lastSyncedLog);

						_waj.UpdateLogInfo();

						_waj.WriteFileHeader();

						if (_waj.Files.Count == 0)
							_waj.CurrentFile = null;

						_waj._locker.ExitWriteLock();

						foreach (var journalFile in journalFiles)
						{
							if (_waj._env.Options.IncrementalBackupEnabled == false)
								journalFile.DeleteOnClose();
						}

						_waj._dataPager.Sync();

					}
					finally
					{
						if (_waj._locker.IsWriteLockHeld)
							_waj._locker.ExitWriteLock();

						foreach (var fullLog in journalFiles)
						{
							fullLog.Release();
						}
					}

					_waj._dataFlushCounter++;

					tx.Commit();
				}
			}

			private Dictionary<long, JournalFile> ReadTransactionsToFlush(long oldestActiveTransaction, ImmutableList<JournalFile> jrnls)
			{
				var pagesToWrite = new Dictionary<long, JournalFile>();

				_lastTransactionHeader = null;
				foreach (var file in jrnls)
				{
					var startPage = file.Number == _lastSyncedLog ? _lastSyncedPage + 1 : 0;
					var journalReader = new JournalReader(file.Pager, startPage, _lastTransactionHeader);

					while (journalReader.ReadOneTransaction())
					{
						_lastTransactionHeader = journalReader.LastTransactionHeader;
						if (_lastTransactionHeader->TransactionId < oldestActiveTransaction)
							break;
						_lastSyncedLog = file.Number;
						_lastSyncedPage = journalReader.LastSyncedPage;
					}

					foreach (var pageNumber in journalReader.TransactionPageTranslation.Keys)
					{
						pagesToWrite[pageNumber] = file;
					}

					if (_lastTransactionHeader != null &&
						_lastTransactionHeader->TransactionId < oldestActiveTransaction)
					{
						// optimization: do not writes pages that have already have newer version in the rest of the journal
						journalReader.TransactionPageTranslation.Clear();
						// read the rest of the journal file
						while (journalReader.ReadOneTransaction())
						{
						}

						foreach (var supercedingPage in journalReader.TransactionPageTranslation.Keys)
						{
							pagesToWrite.Remove(supercedingPage);
						}

						break;
					}

				}
				return pagesToWrite;
			}


			public void UpdateFileHeaderAfterDataFileSync(Transaction tx)
			{
				_waj._fileHeader->TransactionId = _lastTransactionHeader->TransactionId;
				_waj._fileHeader->LastPageNumber = _lastTransactionHeader->LastPageNumber;

				_waj._fileHeader->LogInfo.LastSyncedLog = _lastSyncedLog;
				_waj._fileHeader->LogInfo.LastSyncedLogPage = _lastSyncedPage;
				_waj._fileHeader->LogInfo.DataFlushCounter = _waj._dataFlushCounter;

				tx.State.Root.State.CopyTo(&_waj._fileHeader->Root);
				tx.State.FreeSpaceRoot.State.CopyTo(&_waj._fileHeader->Root);
			}
		}
	}
}
