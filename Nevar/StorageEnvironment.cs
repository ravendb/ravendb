using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using Nevar.Impl;
using Nevar.Impl.FileHeaders;
using Nevar.Trees;
using System.Linq;

namespace Nevar
{
	public unsafe class StorageEnvironment : IDisposable
	{
		private readonly IVirtualPager _pager;
	    private readonly bool _ownsPager;
	    private readonly SliceComparer _sliceComparer;

	    private readonly SemaphoreSlim _txWriter = new SemaphoreSlim(1);
		private readonly ConcurrentDictionary<long, Transaction> _activeTransactions = new ConcurrentDictionary<long, Transaction>();

		private long _transactionsCounter;
		public long NextPageNumber { get; set; }

		public StorageEnvironment(IVirtualPager pager, bool ownsPager = true)
		{
			_pager = pager;
		    _ownsPager = ownsPager;
		    _sliceComparer = NativeMethods.memcmp;

            if (pager.NumberOfAllocatedPages == 0)
            {
                WriteEmptyHeaderPage(_pager.Get(0));
                WriteEmptyHeaderPage(_pager.Get(1));

                NextPageNumber = 2;
                using (var tx = new Transaction(_pager, this, _transactionsCounter + 1, TransactionFlags.ReadWrite))
                {  
                    FreeSpace = Tree.Create(tx, _sliceComparer);
                    Root = Tree.Create(tx, _sliceComparer);
                    tx.Commit();
                }
            }
            else // existing db, let us load it
            {
                // the first two pages are allocated for double buffering tx commits
                var entry = FindLatestFileHeadeEntry();
                NextPageNumber = entry->LastPageNumber + 1;
                _transactionsCounter = entry->TransactionId + 1;
                using (var tx = new Transaction(_pager, this, _transactionsCounter + 1, TransactionFlags.ReadWrite))
                {
                    FreeSpace = Tree.Open(tx, _sliceComparer, &entry->FreeSpace);
                    Root = Tree.Open(tx, _sliceComparer, &entry->Root);

                    tx.Commit();
                }
            }

            FreeSpace.Name = "Free Space";
            Root.Name = "Root";
		}

	    private void WriteEmptyHeaderPage(Page pg)
	    {
	        var fileHeader = ((FileHeader*) pg.Base);
	        fileHeader->MagicMarker = Constants.MagicMarker;
	        fileHeader->Version = Constants.CurrentVersion;
	        fileHeader->TransactionId = 0;
            fileHeader->LastPageNumber = 1;
	        fileHeader->FreeSpace.RootPageNumber = -1;
            fileHeader->Root.RootPageNumber = -1;
	    }

	    private FileHeader* FindLatestFileHeadeEntry()
	    {
	        var fst = _pager.Get(0);
	        var snd = _pager.Get(1);

	        var e1 = GetFileHeaderFrom(fst);
	        var e2 = GetFileHeaderFrom(snd);

	        var entry = e1;
	        if (e2->TransactionId < e1->TransactionId)
	        {
	            entry = e2;
	        }
	        return entry;
	    }

	    private FileHeader* GetFileHeaderFrom(Page p)
	    {
	        var fileHeader = ((FileHeader*) p.Base);
	        if (fileHeader->MagicMarker != Constants.MagicMarker)
	            throw new InvalidDataException("The header page did not start with the magic marker, probably not a db file");
            if(fileHeader->Version != Constants.CurrentVersion)
                throw new InvalidDataException("This is a db file for version " + fileHeader->Version + ", which is not compatible with the current version " + Constants.CurrentVersion);
            if(fileHeader->LastPageNumber >= _pager.NumberOfAllocatedPages)
                throw new InvalidDataException("The last page number is beyond the number of allocated pages");
            if (fileHeader->TransactionId < 0)
                throw new InvalidDataException("The transaction number cannot be negative");
            return fileHeader;
	    }

	    public SliceComparer SliceComparer
		{
			get { return _sliceComparer; }
		}

		public void Dispose()
		{
		    if (_ownsPager)
		        _pager.Dispose();
		}

		public Tree Root { get; private set; }
		public Tree FreeSpace { get; private set; }

		public Transaction NewTransaction(TransactionFlags flags)
		{
		    bool txLockTaken = false;
		    try
		    {
                long txId = _transactionsCounter;
                if (flags.HasFlag(TransactionFlags.ReadWrite))
                {
                    txId = _transactionsCounter + 1;
                    _txWriter.Wait();
                    txLockTaken = true;
                }
                var newTransaction = new Transaction(_pager, this, txId, flags);
                _activeTransactions.TryAdd(txId, newTransaction);
                return newTransaction;
		    }
		    catch (Exception)
		    {
		        if (txLockTaken)
		            _txWriter.Release();
		        throw;
		    }
		}

		public long OldestTransaction
		{
			get { return _activeTransactions.Keys.OrderBy(x => x).FirstOrDefault(); }
		}

		internal void TransactionCompleted(long txId)
		{
			Transaction tx;
		    if (_activeTransactions.TryRemove(txId, out tx) == false)
		        return;

		    if (tx.Flags.HasFlag(TransactionFlags.ReadWrite) == false)
		        return;

		    _transactionsCounter = txId;
		    _txWriter.Release();
		}
	}
}