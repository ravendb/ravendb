using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Nevar.Trees;

namespace Nevar.Impl
{
    public class FreeSpaceCollector
    {
        private readonly StorageEnvironment _env;
        private long _freeSpaceGatheredMinTx;
        private Slice _freeSpaceKey;
        private int _originalFreeSpaceCount;
        private bool _alreadyLookingForFreeSpace;
        private readonly List<long> _freeSpace = new List<long>();
	    private int _lastTransactionPageUsage;
	    private int _allocateFrom;
	    public int _allocateUntil;
	    public FreeSpaceCollector(StorageEnvironment env)
        {
            _env = env;
        }

		public Page TryAllocateFromFreeSpace(Transaction tx, int num)
		{
			if (_env.FreeSpace == null)
				return null;// this can happen the first time FreeSpace tree is created

			if (_alreadyLookingForFreeSpace)
				return null;// can't recursively find free space

			_alreadyLookingForFreeSpace = true;
			try
			{
				while (true)
				{
					if (_freeSpace.Count == 0)
					{
						if (_freeSpaceGatheredMinTx >= tx.Id)
							return null;
						GatherFreeSpace(tx);
						continue;
					}

					var p = TryAllocatingFromFreeSpace(tx, num);
					if (p != null)
						return p;

					if (_freeSpaceGatheredMinTx >= tx.Id)
						return null;
					GatherFreeSpace(tx);
				}
			}
			finally
			{
				_alreadyLookingForFreeSpace = false;
			}
		}

	    private Page TryAllocatingFromFreeSpace(Transaction tx, int num)
	    {
		    var start = 0;
		    var len = 1;
		    for (int i = 1; i < _freeSpace.Count && (len < num); i++)
		    {
			    if (_freeSpace[i - 1] + 1 != _freeSpace[i]) // hole found, try from current page
			    {
				    start = i;
				    len = 1;
				    continue;
			    }
			    len++;
		    }

		    if (len != num)
			    return null;

		    var page = _freeSpace[start];
		    _freeSpace.RemoveRange(start, len);
		    var newPage = tx.Pager.Get(tx, page);
		    newPage.PageNumber = page;
		    return newPage; // return newPage;
	    }

	    public void LastTransactionPageUsage(int pages)
		{
			if (pages == _lastTransactionPageUsage)
				return;

			// if there is a difference, we apply 1/4 the difference to the current value
			// this is to make sure that we don't suddenly spike the required pages per transaction
			// just because of one abnormally large / small transaction
			_lastTransactionPageUsage += (pages - _lastTransactionPageUsage)/4;
		}


        public void SaveOldFreeSpace(Transaction tx)
        {
            if (_freeSpace.Count == _originalFreeSpaceCount)
                return;
            Debug.Assert(_freeSpaceKey != null);
            if (_freeSpace.Count == 0)
            {
                _env.FreeSpace.Delete(tx, _freeSpaceKey);
                return;
            }
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                foreach (var i in _freeSpace)
                {
                    writer.Write(i);
                }
                _freeSpace.Clear(); // this is no longer usable
                ms.Position = 0;
                _env.FreeSpace.Add(tx, _freeSpaceKey, ms);
            }
        }

        /// <summary>
        /// This method will find all the currently free space in the database and make it easily available 
        /// for the transaction. This has to be called _after_ the transaction has already been setup.
        /// </summary>
        private unsafe void GatherFreeSpace(Transaction tx)
        {
            if (tx.Flags.HasFlag(TransactionFlags.ReadWrite) == false)
                throw new InvalidOperationException("Cannot gather free space in a read only transaction");
            if (_freeSpaceGatheredMinTx >= tx.Id)
                return;

            _freeSpaceGatheredMinTx = tx.Id;
            if (_env.FreeSpace == null)
                return;

            var oldestTx = _env.OldestTransaction;

            var toDelete = new List<Slice>();
            using (var iterator = _env.FreeSpace.Iterate(tx))
            {
                if (iterator.Seek(Slice.BeforeAllKeys) == false)
                    return;
#if DEBUG
                var additions = new HashSet<long>();
#endif
                do
                {
                    var node = iterator.Current;
                    var slice = new Slice(node);

                    var txId = slice.ToInt64() >> 8;

                    if (oldestTx != 0 && txId >= oldestTx)
                        break;  // all the free space after this is tied up in active transactions

                    toDelete.Add(slice);
                    var remainingPages = tx.GetNumberOfFreePages(node);

                    using (var data = Tree.StreamForNode(tx, node))
                    using (var reader = new BinaryReader(data))
                    {
                        for (int i = 0; i < remainingPages; i++)
                        {
                            var pageNum = reader.ReadInt64();
                            Debug.Assert(pageNum >= 2 && pageNum <= tx.Pager.NumberOfAllocatedPages);
#if DEBUG
                            var condition = additions.Add(pageNum);
                            Debug.Assert(condition); // free page number HAVE to be unique
#endif
                            _freeSpace.Add(pageNum);
                        }
                    }

                } while (iterator.MoveNext());
            }

            if (toDelete.Count == 0)
                return;

            _freeSpace.Sort();

            _freeSpaceKey = toDelete[0]; // this is always the oldest

            // if we have just one transaction record with free space, no need to touch it, we can
            // just record that and change that if we need to later on
            if (toDelete.Count == 1)
            {
                _originalFreeSpaceCount = _freeSpace.Count;
                return;
            }
            _originalFreeSpaceCount = -1; // force merging of all the available transactions into one transaction free space record

            foreach (var slice in toDelete)
            {
                _env.FreeSpace.Delete(tx, slice);
            }
        }
    }
}