using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Voron.Exceptions;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Scratch
{
	/// <summary>
	/// This class implements the page pool for in flight transaction information
	/// Pages allocated from here are expected to live after the write transaction that 
	/// created them. The pages will be kept around until the flush for the journals
	/// send them to the data file.
	/// 
	/// This class relies on external synchronization and is not meant to be used in multiple
	/// threads at the same time
	/// </summary>
	public unsafe class ScratchBufferPool : IDisposable
	{
		private readonly long _sizeLimit;
		private ScratchBufferFile _current;
		private StorageEnvironmentOptions _options;
		private int _currentScratchNumber = -1;
		private readonly Dictionary<int, ScratchBufferFile> _scratchBuffers = new Dictionary<int, ScratchBufferFile>();

		public ScratchBufferPool(StorageEnvironment env)
		{
			_options = env.Options;
			_sizeLimit = env.Options.MaxScratchBufferSize;
			_current = NextFile();
		}

		public Dictionary<int, PagerState> GetPagerStatesOfAllScratches()
		{
			return _scratchBuffers.ToDictionary(x => x.Key, y => y.Value.PagerState);
		}

		internal long GetNumberOfAllocations(int scratchNumber)
		{
			return _scratchBuffers[scratchNumber].NumberOfAllocations;
		}

		private ScratchBufferFile NextFile()
		{
			_currentScratchNumber++;
			var scratchPager = _options.CreateScratchPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber));
			scratchPager.AllocateMorePages(null, _options.InitialFileSize.HasValue ? Math.Max(_options.InitialFileSize.Value, _options.InitialLogFileSize) : _options.InitialLogFileSize);

			var scratchFile = new ScratchBufferFile(scratchPager, _currentScratchNumber);
			_scratchBuffers.Add(_currentScratchNumber, scratchFile);

			return scratchFile;
		}

		public PagerState GetPagerState(int scratchNumber)
		{
			return _scratchBuffers[scratchNumber].PagerState;
		}

		public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages)
		{
			if (tx == null)
				throw new ArgumentNullException("tx");
			var size = Utils.NearestPowerOfTwo(numberOfPages);

			PageFromScratchBuffer result;
			if (_current.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
				return result;

			long sizeAfterAllocation = _current.SizeAfterAllocation(size);

			if (_scratchBuffers.Count > 1)
			{
				var scratchesToDelete = new List<int>();
				var oldestTransaction = tx.Environment.OldestTransaction;

				// determine how many bytes of older scratches is still in use
				foreach (var olderScratch in _scratchBuffers.Values.Except(new []{_current}))
				{
					var bytesInUse = olderScratch.ActivelyUsedBytes(oldestTransaction);

					if (bytesInUse > 0)
						sizeAfterAllocation += bytesInUse;
					else
						scratchesToDelete.Add(olderScratch.Number);
				}

				// delete inactive scratches
				foreach (var scratchNumber in scratchesToDelete)
				{
					var scratchBufferFile = _scratchBuffers[scratchNumber];
					_scratchBuffers.Remove(scratchNumber);
					scratchBufferFile.Dispose();
				}
			}

			if (sizeAfterAllocation > _sizeLimit)
			{
				var sp = Stopwatch.StartNew();
				// Our problem is that we don't have any available free pages, probably because
				// there are read transactions that are holding things open. We are going to see if
				// there are any free pages that _might_ be freed for us if we wait for a bit. The idea
				// is that we let the read transactions time to complete and do their work, at which point
				// we can continue running. 
				// We start this by forcing a flush, then we are waiting up to the timeout for we are waiting
				// for the read transactions to complete. It is possible that a long running read transaction
				// would in fact generate enough work for us to timeout, but hopefully we can avoid that.

				tx.Environment.ForceLogFlushToDataFile(tx);
				while (sp.ElapsedMilliseconds < tx.Environment.Options.ScratchBufferOverflowTimeout)
				{
					if (_current.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
						return result;
					Thread.Sleep(32);
				}

				if (_current.HasDiscontinuousSpaceFor(tx, size))
				{
					// there is enough space for the requested allocation but the problem is its fragmentation
					// so we will create a new scratch file and will allow to allocate new continuous range from there

					_current = NextFile();

					_current.PagerState.AddRef();
					tx.AddPagerState(_current.PagerState);

					return _current.Allocate(tx, numberOfPages, size);
				}

				string message = string.Format("Cannot allocate more space for the scratch buffer.\r\n" +
											   "Current size is:\t{0:#,#;;0} kb.\r\n" +
											   "Limit:\t\t\t{1:#,#;;0} kb.\r\n" +
											   "Requested Size:\t{2:#,#;;0} kb.\r\n" +
											   "Already flushed and waited for {3:#,#;;0} ms for read transactions to complete.\r\n" +
											   "Do you have a long running read transaction executing?",
					_current.Size / 1024,
					_sizeLimit / 1024,
					(_current.Size + (size * AbstractPager.PageSize)) / 1024,
					sp.ElapsedMilliseconds);
				throw new ScratchBufferSizeLimitException(message);
			}

			// we don't have free pages to give out, need to allocate some
			result = _current.Allocate(tx, numberOfPages, size);

			return result;
		}

		public void Free(int scratchNumber, long page, long asOfTxId, bool ignoreError = false)
		{
			_scratchBuffers[scratchNumber].Free(page, asOfTxId, ignoreError);
		}

		public void Dispose()
		{
			foreach (var scratchBufferFile in _scratchBuffers)
			{
				scratchBufferFile.Value.Dispose();
			}
		}

		public Page ReadPage(int scratchNumber, long p, PagerState pagerState = null)
		{
			return _scratchBuffers[scratchNumber].ReadPage(p, pagerState);
		}

		public byte* AcquirePagePointer(int scratchNumber, long p)
		{
			return _scratchBuffers[scratchNumber].AcquirePagePointer(p);
		}
	}
}