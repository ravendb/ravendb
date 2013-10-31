// -----------------------------------------------------------------------
//  <copyright file="WriteAheadJournal.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron.Impl.Journal
{
	public unsafe class WriteAheadJournal : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly IVirtualPager _dataPager;
		internal readonly Func<long, string> LogName = number => string.Format("{0:D19}.journal", number);

	    private long _currentLogFileSize;
	    private DateTime _lastFile;
		private readonly IList<JournalFile> _splitJournalFiles;

		private long _logIndex = -1;
		private FileHeader* _fileHeader;
		private IntPtr _inMemoryHeader;
		private long _dataFlushCounter = 0;

		internal ImmutableList<JournalFile> Files = ImmutableList<JournalFile>.Empty;
		internal JournalFile CurrentFile;

		public WriteAheadJournal(StorageEnvironment env)
		{
			_env = env;
			_dataPager = _env.Options.DataPager;
			_fileHeader = GetEmptyFileHeader();
			_splitJournalFiles = new List<JournalFile>();
		    _currentLogFileSize = env.Options.InitialLogFileSize;
		}

		internal FileHeader* FileHeader
		{
			get { return _fileHeader; }
		}

		public IVirtualPager DataPager
		{
			get { return _dataPager; }
		}

		private JournalFile NextFile(Transaction tx)
		{
			_logIndex++;

			var logPager = _env.Options.CreateJournalPager(LogName(_logIndex));

	        var now = DateTime.UtcNow ;
            if ((now - _lastFile).TotalSeconds < 90)
            {
                _currentLogFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentLogFileSize * 2);
            }
	        _lastFile = now;

	        logPager.AllocateMorePages(null, _currentLogFileSize);

			var log = new JournalFile(logPager, _logIndex);
			log.AddRef(); // one reference added by a creator - write ahead log
			tx.SetLogReference(log); // and the next one for the current transaction

			Files = Files.Add(log);

			UpdateLogInfo();
			WriteFileHeader();

			return log;
		}

		public void RecoverDatabase(FileHeader* fileHeader, out TransactionHeader* lastTxHeader)
		{
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
			    _currentLogFileSize = pager.NumberOfAllocatedPages;
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
			var logs = Files; // thread safety copy

			for (var i = logs.Count - 1; i >= 0; i--)
			{
				var page = logs[i].ReadPage(tx, pageNumber);
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
				// it will have th following transaction markers in the headers
				// log_1: [Start|Split|Commit] log_2: [Split|Commit]

				CurrentFile.TransactionSplit(tx);
				_splitJournalFiles.Add(CurrentFile);

				CurrentFile = NextFile(tx);

				CurrentFile.TransactionSplit(tx);
			}

			return CurrentFile.Allocate(startPage, numberOfPages);
		}

		public IList<Tuple<Page, int>> AllocateForOverflow(Transaction tx, long startPage, int numberOfPages)
		{
			var pages = new List<Tuple<Page, int>>();

			if (CurrentFile.AvailablePages < numberOfPages)
			{
				CurrentFile.TransactionSplit(tx);
				_splitJournalFiles.Add(CurrentFile);

				while (numberOfPages > 0)
				{
					CurrentFile = NextFile(tx);
					CurrentFile.TransactionSplit(tx);

					var allocatedPages = Math.Min((int)CurrentFile.AvailablePages, numberOfPages);

					pages.Add(new Tuple<Page, int>(CurrentFile.Allocate(startPage, allocatedPages), allocatedPages));

					startPage += allocatedPages;
					numberOfPages -= allocatedPages;

					if (numberOfPages > 0)
						_splitJournalFiles.Add(CurrentFile);
				}

				return pages;
			}

			pages.Add(new Tuple<Page, int>(CurrentFile.Allocate(startPage, numberOfPages), numberOfPages));
			return pages;
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

		public class JournalApplicator
		{
			private readonly WriteAheadJournal _waj;
			private readonly long _oldestActiveTransaction;
			private long _lastSyncedLog;
			private long _lastSyncedPage;
			private TransactionHeader* _lastTransactionHeader;

			public JournalApplicator(WriteAheadJournal waj, long oldestActiveTransaction)
			{
				_waj = waj;
				_oldestActiveTransaction = oldestActiveTransaction;
			}

			public void ApplyLogsToDataFile()
			{
				using (var tx = _waj._env.NewTransaction(TransactionFlags.Read))
				{
					var jrnls = _waj.Files; // thread safety copy

					if (jrnls.Count == 0)
						return;

					var pagesToWrite = ReadTransactionsToFlush(_oldestActiveTransaction, jrnls);

					if (pagesToWrite.Count == 0)
						return;

					Debug.Assert(_lastTransactionHeader != null);

					var sortedPages = pagesToWrite.OrderBy(x => x.Key)
												  .Select(x => x.Value.ReadPage(null, x.Key))
												  .ToList();

					var last = sortedPages.Last();

					_waj.DataPager.EnsureContinuous(null, last.PageNumber,
													last.IsOverflow
														? _waj._env.Options.DataPager.GetNumberOfOverflowPages(
															last.OverflowSize)
														: 1);

					foreach (var page in sortedPages)
					{
						_waj.DataPager.Write(page);
					}

					_waj.DataPager.Sync();

					UpdateFileHeaderAfterDataFileSync(tx);

					var journalFiles = jrnls.RemoveAll(x => x.Number >= _lastSyncedLog);
					foreach (var fullLog in journalFiles)
					{
						if (_waj._env.Options.IncrementalBackupEnabled == false)
							fullLog.DeleteOnClose();
					}

					_waj.Files = _waj.Files.RemoveAll(x => x.Number < _lastSyncedLog);

					_waj.UpdateLogInfo();

					foreach (var fullLog in journalFiles)
					{
						fullLog.Release();
					}

					if (_waj.Files.Count == 0)
						_waj.CurrentFile = null;

					_waj.WriteFileHeader();

					_waj._dataFlushCounter++;

					tx.Commit();
				}
			}

			private Dictionary<long, JournalFile> ReadTransactionsToFlush(long oldestActiveTransaction, ImmutableList<JournalFile> jrnls)
			{
				_lastSyncedLog = _waj._fileHeader->LogInfo.LastSyncedLog;
				_lastSyncedPage = _waj._fileHeader->LogInfo.LastSyncedLogPage;

				Debug.Assert(jrnls.First().Number >= _lastSyncedLog);

				var pagesToWrite = new Dictionary<long, JournalFile>();

				_lastTransactionHeader = null;
				foreach (var file in jrnls)
				{
					var startPage = file.Number == _waj._fileHeader->LogInfo.LastSyncedLog ? _waj._fileHeader->LogInfo.LastSyncedLogPage + 1 : 0;
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

					if (_lastTransactionHeader != null && _lastTransactionHeader->TransactionId < oldestActiveTransaction)
						break;

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