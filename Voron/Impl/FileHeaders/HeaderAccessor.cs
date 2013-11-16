// -----------------------------------------------------------------------
//  <copyright file="HeaderAccessor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Voron.Impl.FileHeaders
{
	public unsafe delegate void ModifyHeaderAction(FileHeader* ptr);

	public unsafe class HeaderAccessor : IDisposable
	{
		private readonly IVirtualPager[] _pagers;
		private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();
		private long _revision = -1;

		public HeaderAccessor(StorageEnvironment env)
		{
			_pagers = new IVirtualPager[2]
				{
					env.Options.HeaderPagers.Item1,
					env.Options.HeaderPagers.Item2
				};
		}

		public bool Initialize()
		{
			Debug.Assert(_pagers[0].AllocatedSize == _pagers[1].AllocatedSize);

			var @new = _pagers[0].AllocatedSize == 0 && _pagers[1].AllocatedSize == 0;

			foreach (var pager in _pagers)
			{
				if (@new)
					pager.AllocateMorePages(null, sizeof(FileHeader));

				var header = (FileHeader*)pager.PagerState.Base;

				if (@new)
				{
					FillInEmptyHeader(header);
					pager.Sync();
				}
				else
				{
					if (header->MagicMarker != Constants.MagicMarker)
						throw new InvalidDataException(
							"The header page did not start with the magic marker, probably not a db file");
					if (header->Version != Constants.CurrentVersion)
						throw new InvalidDataException("This is a db file for version " + header->Version +
													   ", which is not compatible with the current version " +
													   Constants.CurrentVersion);
					// TODO arek
					//if (header->LastPageNumber >= _dataPager.NumberOfAllocatedPages)
					//	throw new InvalidDataException("The last page number is beyond the number of allocated pages");
					if (header->TransactionId < 0)
						throw new InvalidDataException("The transaction number cannot be negative");

					if (header->HeaderRevision > _revision)
						_revision = header->HeaderRevision;
				}

				pager.PagerState.AddRef();
			}

			return @new;
		}

		public FileHeader* Get()
		{
			_locker.EnterReadLock();
			try
			{
				var header = (FileHeader*) _pagers[_revision & 1].PagerState.Base;

				return header;
			}
			finally
			{
				_locker.ExitReadLock();
			}
		}

		public void Modify(ModifyHeaderAction modifyAction)
		{
			_locker.EnterWriteLock();
			try
			{
				var lastModified = _pagers[_revision & 1].PagerState.Base;

				_revision++;

				var pager = _pagers[_revision & 1];
				var headerPtr =  pager.PagerState.Base;
				var header = (FileHeader*) headerPtr;

				NativeMethods.memcpy(headerPtr, lastModified, sizeof(FileHeader));

				modifyAction(header);

				header->HeaderRevision = _revision;

				pager.Sync();
			}
			finally
			{
				_locker.ExitWriteLock();
			}
		}

		private static void FillInEmptyHeader(FileHeader* header)
		{
			header->MagicMarker = Constants.MagicMarker;
			header->Version = Constants.CurrentVersion;
			header->HeaderRevision = -1;
			header->TransactionId = 0;
			header->LastPageNumber = 1;
			header->FreeSpace.RootPageNumber = -1;
			header->Root.RootPageNumber = -1;
			header->Journal.CurrentJournal = -1;
			header->Journal.JournalFilesCount = 0;
			header->Journal.LastSyncedJournal = -1;
			header->Journal.LastSyncedJournalPage = -1;
			header->IncrementalBackup.LastBackedUpJournal = -1;
			header->IncrementalBackup.LastBackedUpJournalPage = -1;
			header->IncrementalBackup.LastCreatedJournal = -1;
		}

		public void Dispose()
		{
			foreach (var pager in _pagers)
			{
				pager.PagerState.Release();
			}
		}
	}
}