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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Voron.Impl.Backup;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron.Impl.Journal
{
	public unsafe class WriteAheadJournal : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly IVirtualPager _dataPager;
		
		private long _currentJournalFileSize;
		private DateTime _lastFile;

		private long _logIndex = -1;
		private FileHeader* _fileHeader;
		private IntPtr _inMemoryHeader;
		private long _dataFlushCounter;

		internal ImmutableList<JournalFile> Files = ImmutableList<JournalFile>.Empty;
		internal JournalFile CurrentFile;

		private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();
		private readonly object _fileHeaderProtector = new object();

		public WriteAheadJournal(StorageEnvironment env)
		{
			_env = env;
			_dataPager = _env.Options.DataPager;
			_fileHeader = GetEmptyFileHeader();
			_currentJournalFileSize = env.Options.InitialLogFileSize;
		}

		private JournalFile NextFile(Transaction tx, int numberOfPages = 1)
		{
			_logIndex++;

			var logPager = _env.Options.CreateJournalPager(_logIndex);

			var now = DateTime.UtcNow;
			if ((now - _lastFile).TotalSeconds < 90)
			{
				_currentJournalFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentJournalFileSize * 2);
			}
			var actualLogSize = _currentJournalFileSize;
			var minRequiredsize = numberOfPages * logPager.PageSize;
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

			lock (_fileHeaderProtector)
			{
				_dataPager.Sync(); // we have to flush the new log information to disk, may be expensive, so we do it outside the lock
			}

			return log;
		}

		public void RecoverDatabase(FileHeader* fileHeader, out TransactionHeader* lastTxHeader,out bool requireHeaderUpdate)
		{
			// note, we don't need to do any concurrency here, happens as a single threaded
			// fashion on db startup
			requireHeaderUpdate = false;

			_fileHeader = CopyFileHeader(fileHeader);
			var logInfo = _fileHeader->Journal;

			lastTxHeader = null;

			if (logInfo.JournalFilesCount == 0)
			{
				return;
			}

			var oldestLogFileStillInUse = logInfo.CurrentJournal - logInfo.JournalFilesCount + 1;
			if (_env.Options.IncrementalBackupEnabled == false)
			{
				// we want to check that we cleanup old log files if they aren't needed
				// this is more just to be safe than anything else, they shouldn't be there.
				var unusedfiles = oldestLogFileStillInUse;
				while (true)
				{
					unusedfiles--;
					if (_env.Options.TryDeleteJournalPager(unusedfiles) == false)
						break;
				}
				
			}

			for (var logNumber = oldestLogFileStillInUse; logNumber <= logInfo.CurrentJournal; logNumber++)
			{
				var pager = _env.Options.CreateJournalPager(logNumber);
				RecoverCurrentJournalSize(pager);
				var log = new JournalFile(pager, logNumber);

				long startRead = 0;

				if (log.Number == logInfo.LastSyncedJournal)
					startRead = logInfo.LastSyncedJournalPage + 1;

				lastTxHeader = log.RecoverAndValidate(startRead, lastTxHeader);

                log.AddRef(); // creator reference - write ahead log
                Files = Files.Add(log);

                if (log.RequireHeaderUpdate) //this should prevent further loading of transactions
                {
                    requireHeaderUpdate = true;
                    break;
                }

			    if (lastTxHeader == null && log.AvailablePages == 0) // an empty log file, or one which was completely used
			    {
                    log.Dispose();
			    }
			}

		    if (requireHeaderUpdate)
		    {
		        logInfo.CurrentJournal = Files.Count - 1;

                // we want to check that we cleanup newer log files, since everything from
                // the current file is considered corrupted
                var badJournalFiles = logInfo.CurrentJournal;
                while (true)
                {
                    badJournalFiles++;
                    if (_env.Options.TryDeleteJournalPager(badJournalFiles) == false)
                        break;
                }
		    }

			_logIndex = logInfo.CurrentJournal;
			_dataFlushCounter = logInfo.DataFlushCounter + 1;


			if (Files.IsEmpty == false)
			{
				var lastFile = Files.Last();
				if (lastFile.AvailablePages >= 2)
					// it must have at least one page for the next transaction header and one page for data
					CurrentFile = lastFile;
			}

			if (requireHeaderUpdate)
			{
				UpdateLogInfo();
				WriteFileHeader();
			}
		}

		private void RecoverCurrentJournalSize(IVirtualPager pager)
		{
			var journalSize = AbstractPager.NearestPowerOfTwo(pager.NumberOfAllocatedPages * pager.PageSize);
			if (journalSize >= _env.Options.MaxLogFileSize) // can't set for more than the max log file size
				return;

		    _currentJournalFileSize = journalSize;
		}

		public void UpdateLogInfo()
		{
			_fileHeader->Journal.CurrentJournal = Files.Count > 0 ? _logIndex : -1;
			_fileHeader->Journal.JournalFilesCount = Files.Count;
			_fileHeader->Journal.DataFlushCounter = _dataFlushCounter;

			_fileHeader->IncrementalBackup.LastCreatedJournal = _logIndex;
		}

		internal void WriteFileHeader(long? pageToWriteHeader = null)
		{
			lock (_fileHeaderProtector)
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
				header->Journal = _fileHeader->Journal;
				header->IncrementalBackup = _fileHeader->IncrementalBackup;

				_dataPager.Write(fileHeaderPage, page);
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

			_locker.EnterReadLock();
			try
			{
				// write transactions can read directly from logs
				for (var i = Files.Count - 1; i >= 0; i--)
				{
					var page = Files[i].ReadPage(pageNumber);
					if (page != null)
						return page;
				}
			}
			finally
			{
				_locker.ExitReadLock();
			}
			
			return null;
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
			header->Journal.DataFlushCounter = -1;
			header->Journal.CurrentJournal = -1;
			header->Journal.JournalFilesCount = 0;
			header->Journal.LastSyncedJournal = -1;
			header->Journal.LastSyncedJournalPage = -1;
			header->IncrementalBackup.LastBackedUpJournal = -1;
			header->IncrementalBackup.LastBackedUpJournalPage = -1;
			header->IncrementalBackup.LastCreatedJournal = -1;

			return header;
		}

		private FileHeader* CopyFileHeader(FileHeader* fileHeader)
		{
			Debug.Assert(_inMemoryHeader != IntPtr.Zero);

			NativeMethods.memcpy((byte*)_inMemoryHeader, (byte*)fileHeader, sizeof(FileHeader));

			return (FileHeader*)_inMemoryHeader;
		}

		public JournalInfo GetCurrentJournalInfo()
		{
			_locker.EnterReadLock();
			try
			{
				return _fileHeader->Journal;
			}
			finally
			{
				_locker.ExitReadLock();
			}
		}

		public IncrementalBackupInfo GetIncrementalBackupInfo()
		{
			_locker.EnterReadLock();
			try
			{
				return _fileHeader->IncrementalBackup;
			}
			finally
			{
				_locker.ExitReadLock();
			}
		}

		public void UpdateAfterIncrementalBackup(long lastBackedUpJournalFile, long lastBackedUpJournalFilePage)
		{
			_locker.EnterWriteLock();
			try
			{
				_fileHeader->IncrementalBackup.LastBackedUpJournal = lastBackedUpJournalFile;
				_fileHeader->IncrementalBackup.LastBackedUpJournalPage = lastBackedUpJournalFilePage;

				WriteFileHeader();
			}
			finally
			{
				_locker.ExitWriteLock();
			}

			lock (_fileHeaderProtector)
			{
				_dataPager.Sync(); // we have to flush the new backup information to disk
			}
		}

		public List<LogSnapshot> GetSnapshots()
		{
			return Files.Select(x => x.GetSnapshot()).ToList();
		}


		public long SizeOfUnflushedTransactionsInJournalFile()
		{
			_locker.EnterReadLock();
			try
			{
				var currentFile = CurrentFile;
				if (currentFile == null)
					return 0;

				using (var tx = _env.NewTransaction(TransactionFlags.Read))
				{
					var lastSyncedLog = _fileHeader->Journal.LastSyncedJournal;
					var lastSyncedLogPage = _fileHeader->Journal.LastSyncedJournalPage;

					var sum = Files.Sum(file =>
					{
						if(file.Number == lastSyncedLog && lastSyncedLog != 0)
							return lastSyncedLogPage - currentFile.WritePagePosition - 1;
						return file.WritePagePosition == 0 ? 0 : file.WritePagePosition - 1;
					});

					tx.Commit();
					return sum;
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
                        if(_jrnls.Count == 0)
                            return; // nothing to do
						_lastSyncedLog = _waj._fileHeader->Journal.LastSyncedJournal;
						_lastSyncedPage = _waj._fileHeader->Journal.LastSyncedJournalPage;
						Debug.Assert(_jrnls.First().Number >= _lastSyncedLog);
					}
					finally
					{
						_waj._locker.ExitReadLock();
					}

					var pagesToWrite = ReadTransactionsToFlush(_oldestActiveTransaction, _jrnls);

					if (pagesToWrite.Count == 0)
						return;

					Debug.Assert(_lastTransactionHeader != null);

					var sortedPages = pagesToWrite.OrderBy(x => x.Key)
												  .Select(x => x.Value.ReadPage(x.Key))
												  .ToList();

					var last = sortedPages.Last();

					var lastPage = last.IsOverflow == false ? 1 :
						_waj._env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

					_waj._dataPager.EnsureContinuous(null, last.PageNumber, lastPage);

					foreach (var page in sortedPages)
					{
						_waj._dataPager.Write(page);
					}

				    var unusedJournalFiles = GetUnusedJournalFiles();

				    // we want to hold the write lock for as little time as possible, therefor
					// what we do is take the lock, write the appropriate metadata, then exit the lock
					// the actual fsync (expensive) is happening outside the lock, and when can then 
					// decide to clear the old logs if we want to.
					_waj._locker.EnterWriteLock();
					try
					{
						UpdateFileHeaderAfterDataFileSync(tx);

					    var lastJournalFileToRemove = unusedJournalFiles.LastOrDefault();
                        if(lastJournalFileToRemove != null)
					        _waj.Files = _waj.Files.RemoveAll(x => x.Number <= lastJournalFileToRemove.Number);

						_waj.UpdateLogInfo();

						_waj.WriteFileHeader();

						if (_waj.Files.Count == 0)
						{
							_waj.CurrentFile = null;
						}
						
						_waj._locker.ExitWriteLock();

						foreach (var journalFile in unusedJournalFiles)
						{
							if (_waj._env.Options.IncrementalBackupEnabled == false)
								journalFile.DeleteOnClose();
						}

						lock (_waj._fileHeaderProtector)
						{
							_waj._dataPager.Sync();
						}
					}
					finally
					{
						if (_waj._locker.IsWriteLockHeld)
							_waj._locker.ExitWriteLock();

						foreach (var fullLog in unusedJournalFiles)
						{
							fullLog.Release();
						}
					}

					_waj._dataFlushCounter++;

					tx.Commit();
				}
			}

		    private List<JournalFile> GetUnusedJournalFiles()
		    {
		        var unusedJournalFiles = new List<JournalFile>();
		        foreach (var j in _jrnls)
		        {
		            if (j.Number > _lastSyncedLog) // after the last log we synced, nothing to do here
		                continue;
		            if (j.Number == _lastSyncedLog) // we are in the last log we synced
		            {
		                // if we didn't get to end, or if there are more pages to be used here, ignore it
		                if (_lastSyncedPage != (j.WritePagePosition-1) || j.AvailablePages != 0)
		                    continue;
		            }
		            unusedJournalFiles.Add(j);
		        }
		        return unusedJournalFiles;
		    }

		    private Dictionary<long, JournalFile> ReadTransactionsToFlush(long oldestActiveTransaction, ImmutableList<JournalFile> jrnls)
			{
				var pagesToWrite = new Dictionary<long, JournalFile>();

				_lastTransactionHeader = null;
				foreach (var file in jrnls)
				{
					var startPage = file.Number == _lastSyncedLog ? _lastSyncedPage + 1 : 0;
					var journalReader = new JournalReader(file.Pager, startPage, _lastTransactionHeader);

					// we need to ensure that we aren't overwriting pages that are currently being seen by existing transactions
					Func<TransactionHeader, bool> readUntil =
						header => oldestActiveTransaction == 0 || // it means there is no active transaction
									header.TransactionId < oldestActiveTransaction; // only transactions older than currently the oldest active one

					while (journalReader.ReadOneTransaction(readUntil))
					{
						_lastTransactionHeader = journalReader.LastTransactionHeader;
						_lastSyncedLog = file.Number;
						_lastSyncedPage = journalReader.LastSyncedPage;
					}

					foreach (var pageNumber in journalReader.TransactionPageTranslation.Keys)
					{
						pagesToWrite[pageNumber] = file;
					}

					var hasJournalMoreWrittenPages = journalReader.LastSyncedPage < (file.WritePagePosition - 1);

					if (journalReader.TransactionPageTranslation.Count > 0 && hasJournalMoreWrittenPages)
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
					}

					if(journalReader.EncounteredStopCondition)
						break;
				}
				return pagesToWrite;
			}


			public void UpdateFileHeaderAfterDataFileSync(Transaction tx)
			{
				_waj._fileHeader->TransactionId = _lastTransactionHeader->TransactionId;
				_waj._fileHeader->LastPageNumber = _lastTransactionHeader->LastPageNumber;

				_waj._fileHeader->Journal.LastSyncedJournal = _lastSyncedLog;
				_waj._fileHeader->Journal.LastSyncedJournalPage = _lastSyncedPage == 0 ? -1 : _lastSyncedPage;
				_waj._fileHeader->Journal.DataFlushCounter = _waj._dataFlushCounter;

				tx.State.Root.State.CopyTo(&_waj._fileHeader->Root);
				tx.State.FreeSpaceRoot.State.CopyTo(&_waj._fileHeader->FreeSpace);
			}
		}

		public void Clear()
		{
			_locker.EnterWriteLock();
			try
			{
				Files.ForEach(x => x.Release());
				Files = Files.Clear();
				CurrentFile = null;
			}
			finally
			{
				_locker.ExitWriteLock();
			}
		}

	    public void WriteToJournal(Transaction tx, int pageCount)
	    {
	        if (CurrentFile == null || CurrentFile.AvailablePages < pageCount)
	        {
	            CurrentFile = NextFile(tx, pageCount);
            }
	        CurrentFile.Write(tx, pageCount);
	        if (CurrentFile.AvailablePages == 0)
	        {
	            CurrentFile = null;
	        }
	    }
	}
}
