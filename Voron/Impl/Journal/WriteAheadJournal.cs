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
		private readonly Func<long, string> _logName = number => string.Format("{0:D19}.journal", number);

		private JournalFile _splitJournalFile;
		private long _logIndex = -1;
		private FileHeader* _fileHeader;
		private IntPtr _inMemoryHeader;
		private long _dataFlushCounter = 0;
		private bool _disabled;

		internal ImmutableList<JournalFile> Files = ImmutableList<JournalFile>.Empty;
		internal JournalFile CurrentFile;

		public WriteAheadJournal(StorageEnvironment env)
		{
			_env = env;
			_dataPager = _env.Options.DataPager;
			_fileHeader = GetEmptyFileHeader();
		}

		private JournalFile NextFile(Transaction tx)
		{
			_logIndex++;

			var logPager = _env.Options.CreateLogPager(_logName(_logIndex));

			logPager.AllocateMorePages(null, _env.Options.LogFileSize);

			var log = new JournalFile(logPager, _logIndex);
			log.AddRef(); // one reference added by a creator - write ahead log
			tx.SetLogReference(log); // and the next one for the current transaction

			Files = Files.Add(log);

			UpdateLogInfo();
			WriteFileHeader();

			return log;
		}

		public bool TryRecover(FileHeader* fileHeader, out TransactionHeader* lastTxHeader)
		{
			_fileHeader = fileHeader;
			var logInfo = fileHeader->LogInfo;

			lastTxHeader = null;

			if (logInfo.LogFilesCount == 0)
			{
				return false;
			}

			for (var logNumber = logInfo.RecentLog - logInfo.LogFilesCount + 1; logNumber <= logInfo.RecentLog; logNumber++)
			{
				var pager = _env.Options.CreateLogPager(_logName(logNumber));

				if (pager.NumberOfAllocatedPages != (_env.Options.LogFileSize/pager.PageSize))
					throw new InvalidDataException("Log file " + _logName(logNumber) + " should contain " +
                                                   (_env.Options.LogFileSize / pager.PageSize) + " pages, while it has " +
					                               pager.NumberOfAllocatedPages + " pages allocated.");
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

			return true;
		}

		public void UpdateLogInfo()
		{
			_fileHeader->LogInfo.RecentLog = Files.Count > 0 ? _logIndex : -1;
			_fileHeader->LogInfo.LogFilesCount = Files.Count;
			_fileHeader->LogInfo.DataFlushCounter = _dataFlushCounter;
		}

		public void UpdateFileHeaderAfterDataFileSync()
		{
            //_fileHeader->TransactionId = commitPoint.TxId;
            //_fileHeader->LastPageNumber = commitPoint.TxLastPageNumber;

            //_fileHeader->LogInfo.LastSyncedLog = commitPoint.LogNumber;
            //_fileHeader->LogInfo.LastSyncedLogPage = commitPoint.LastWrittenLogPage;
            //_fileHeader->LogInfo.DataFlushCounter = _dataFlushCounter;

            //_env.FreeSpaceHandling.CopyStateTo(&_fileHeader->FreeSpace);
            //_env.Root.State.CopyTo(&_fileHeader->Root);

            throw new NotImplementedException("Fix me");

		}

		internal void WriteFileHeader(long? pageToWriteHeader = null)
		{
            var fileHeaderPage = _dataPager.TempPage;

		    NativeMethods.memset((fileHeaderPage.Base + Constants.PageHeaderSize), 0,
		                         _dataPager.PageSize - Constants.PageHeaderSize);

		    if (pageToWriteHeader == null)
		        fileHeaderPage.PageNumber = _dataFlushCounter & 1;
		    else
		        fileHeaderPage.PageNumber = pageToWriteHeader.Value;

		    var header = (FileHeader*) (fileHeaderPage.Base + Constants.PageHeaderSize);
		    header->MagicMarker = Constants.MagicMarker;
		    header->Version = Constants.CurrentVersion;
		    header->TransactionId = _fileHeader->TransactionId;
		    header->LastPageNumber = _fileHeader->LastPageNumber;

		    header->Root = _fileHeader->Root;
		    header->FreeSpace = _fileHeader->FreeSpace;
		    header->LogInfo = _fileHeader->LogInfo;

		    _dataPager.Write(fileHeaderPage);
		}

	    public void TransactionBegin(Transaction tx)
		{
			if(_disabled)
				return;
			
			if (CurrentFile == null)
				CurrentFile = NextFile(tx);

			if (_splitJournalFile != null) // last split transaction was not committed
			{
				Debug.Assert(_splitJournalFile.LastTransactionCommitted == false);
				CurrentFile = _splitJournalFile;
				_splitJournalFile = null;
			}

			CurrentFile.TransactionBegin(tx);
		}

		public void TransactionCommit(Transaction tx)
		{
			if(_disabled)
				return;

			if (_splitJournalFile != null)
			{
				_splitJournalFile.TransactionCommit(tx);
				_splitJournalFile = null;
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
				for (var i = tx.LogSnapshots.Count -1; i >= 0; i--)
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
				if (_splitJournalFile != null) // we are already in a split transaction and don't allow to spread a transaction over more than two log files
					throw new InvalidOperationException(
						"Transaction attempted to put data in more than two log files. It's not allowed. The transaction is too large.");

				// here we need to mark that transaction is split in both log files
				// it will have th following transaction markers in the headers
				// log_1: [Start|Split] log_2: [Split|Commit]

				CurrentFile.TransactionSplit(tx);
				_splitJournalFile = CurrentFile;

				CurrentFile = NextFile(tx);

				CurrentFile.TransactionSplit(tx);
			}

			return CurrentFile.Allocate(startPage, numberOfPages);
		}

		public void ApplyLogsToDataFile()
		{
            var processingLogs = Files; // thread safety copy

            if (processingLogs.Count == 0)
				return;

			var lastSyncedLog = _fileHeader->LogInfo.LastSyncedLog;
			var lastSyncedPage = _fileHeader->LogInfo.LastSyncedLogPage;

			Debug.Assert(processingLogs.First().Number >= lastSyncedLog);

			var pagesToWrite = new Dictionary<long, Page>();
		    var lastLogFile = -1;

            // TODO: FIXME
            //for (var i = recentLogIndex; i >= 0; i--)
            //{
            //    var log = processingLogs[i];

            //    foreach (var pageNumber in log.GetModifiedPages(log.Number == lastSyncedLog ? lastSyncedPage : (long?) null))
            //    {
            //        if (pagesToWrite.ContainsKey(pageNumber) == false)
            //        {
            //            pagesToWrite[pageNumber] = log.ReadPage(null, pageNumber);
            //        }
            //    }
            //}

			var sortedPages = pagesToWrite.OrderBy(x => x.Key).Select(x => x.Value).ToList();

			if(sortedPages.Count == 0)
				return;

			var last = sortedPages.Last();

			_dataPager.EnsureContinuous(null, last.PageNumber, last.IsOverflow ? _env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize) : 1);

			foreach (var page in sortedPages)
			{
				_dataPager.Write(page);
			}

			_dataPager.Sync();

			UpdateFileHeaderAfterDataFileSync();

            var fullLogs = processingLogs.GetRange(0, lastLogFile);

			foreach (var fullLog in fullLogs)
			{
				if (_env.Options.DeleteUnusedLogFiles)
					fullLog.DeleteOnClose();
			}

			UpdateLogInfo();

            Files = Files.RemoveRange(0, lastLogFile);

			foreach (var fullLog in fullLogs)
			{
				fullLog.Release();
			}

			if (Files.Count == 0)
				CurrentFile = null;

			WriteFileHeader();

			_dataFlushCounter++;
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

		    var header = (FileHeader*) _inMemoryHeader;

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

		public List<LogSnapshot> GetSnapshots()
		{
			return Files.Select(x => x.GetSnapshot()).ToList();
		} 
	}
}