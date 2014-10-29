using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Voron.Trees;

namespace Voron.Impl
{
    /// <summary>
    /// Free space is store in Voron in one of two ways:
    /// 
    /// - Free Space by transaction (all the pages freed in a particular transaction)
    /// - Free Space by section
    /// 
    /// Sections are 4MB ranges of the file. All the free space in a section is gathered up
    /// when a transaction is freed and written to the section.
    /// 
    /// In addition to that, there is also the _current_ section. All the free space allocations are
    /// done from the current section. 
    /// 
    /// When all the free space is gone from the current section, we select the next appropriate section.
    /// This would be the next section of the file (forward wrapping order) that has enough free pages.
    /// 
    /// Ideally, we want to re-use the section for multiple transactions, so we need to find a large one.
    /// We do that by finding a section with a minimum applicable amount, then search the next 256 (about 1GB) 
    /// sections for a section with higher count of free pages.
    /// 
    /// The structure of a transaction in storage is:
    /// 
    /// key - tx/* (always increasing)
    /// value - int64 - tx id, int64* - freed pages
    /// 
    /// The structure of a section in storage is:
    /// 
    /// key = sect/[section id] (lexically sorted)
    /// value - int64 id, int32 largestSeq, int32 pageCount,  int64* - freed pages
    /// 
    /// </summary>
    public unsafe class FreeSpaceRepository : IFreeSpaceRepository
    {
        private readonly StorageEnvironment _env;
        private int _lastTransactionPageUsage;

        public class Section
        {
            public Slice Key;
            public long Id;
            public ConsecutiveSequences Sequences = new ConsecutiveSequences();
        }

        public class FreedTransaction
        {
            public Slice Key;
            public long Id;
            public List<long> Pages;
        }

        private readonly LinkedList<FreedTransaction> _freedTransactions = new LinkedList<FreedTransaction>();
        private readonly List<Slice> _recordsToDelete = new List<Slice>();
        private readonly List<Section> _recordsToUpdate = new List<Section>();
        private readonly List<Slice> _recordsToSkip = new List<Slice>();

        private Slice _currentKey;
        private bool _currentChanged;
        private readonly Slice _sectionsPrefix = "sect/";
        private readonly Slice _txPrefix = "tx/";
        private int _minimumFreePagesInSection;
        private bool _minimumFreePagesInSectionSet;
        private bool _writingFreSpace;
        private Section _current;

        public FreeSpaceRepository(StorageEnvironment env)
        {
            _env = env;
        }

        public Page TryAllocateFromFreeSpace(Transaction tx, int num)
        {
            if (_env.State.FreeSpaceRoot == null)
                return null;// this can happen the first time FreeSpaceRoot tree is created

            long page;
            if (_current == null ||
                _current.Sequences.TryAllocate(num, out page) == false)
            {
                if (_current != null)
                {
                    // now we need to decide whatever we discard the current section (in favor of a better one?)
                    // or just let this write go to the end of the file
                    if (_current.Sequences.Count > _lastTransactionPageUsage)
                    {
                        // we still have a lot of free pages here we can use, let us continue using this section
                        return null;
                    }
                }
                // need to find a new one
                var old = _current;
                try
                {
                    if (SetupNextSection(tx, num, _currentKey) == false)
                    {
                        _currentKey = null;
                        _current = null;
                        _currentChanged = false;
                        return null;
                    }
                }
                finally
                {
                    DiscardSection(old);
                }

                return TryAllocateFromFreeSpace(tx, num);
            }

            _currentChanged = true;
            var newPage = tx.Pager.Get(tx, page);
            newPage.PageNumber = page;
            return newPage;
        }

        private void DiscardSection(Section old)
        {
            if (old == null)
                return;

            _recordsToSkip.Add(old.Key);

            // because this is called during allocation, we can't 
            // do write ops on the tree, instead, we register this
            // to be done during transaction flush
            if (old.Sequences.Count == 0)
            {
                _recordsToDelete.Add(old.Key);
                return;
            }
            _recordsToUpdate.Add(old);
        }

        public long GetFreePageCount()
        {
            long freePages = 0;
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            using (var it = _env.State.FreeSpaceRoot.Iterate(tx))
            {
                if (it.Seek(Slice.BeforeAllKeys) == false)
                {
                    return 0;
                }
                do
                {
                    if (it.CurrentKey.StartsWith(_sectionsPrefix, _env.SliceComparer) || it.CurrentKey.StartsWith(_txPrefix, _env.SliceComparer))
                    {
                        var dataSize = it.GetCurrentDataSize();
                        freePages += dataSize / sizeof(long) - 1;
                    }
                    else
                    {
                        Debug.Assert(false, "invalid key in free space tree: " + it.CurrentKey);
                    }

                } while (it.MoveNext());
            }
            return freePages;

        }

        private bool SetupNextSection(Transaction tx, int num, Slice key)
        {
            if (_writingFreSpace)
                return false; // can't find next section when we are already writing a section
            NodeHeader* current = null;
            bool hasMatch = key != null &&
                TryFindSection(tx, num, key, key, null, out current);
            if (hasMatch == false) // wrap to the beginning 
            {
                if (TryFindSection(tx, num, key, _sectionsPrefix, key, out current) == false)
                {
                    return false;
                }
            }
            Debug.Assert(current != null);

            _currentKey = new Slice(current).Clone();
            _current = new Section
                {
                    Key = _currentKey,
                    Sequences = new ConsecutiveSequences(),
                };

            using (var stream = NodeHeader.Stream(tx, current))
            using (var reader = new BinaryReader(stream))
            {
                _current.Id = reader.ReadInt64();
                var largestSeq = reader.ReadInt32();
                Debug.Assert(largestSeq >= num);
                var pageCount = reader.ReadInt32();
                for (var i = 1; i < pageCount; i++)
                {
                    _current.Sequences.Add(reader.ReadInt64());
                }
            }

            Debug.Assert(_currentKey != null);
            _currentChanged = false;

            return true;
        }

        private bool TryFindSection(Transaction tx, int minSeq, Slice currentKey, Slice start, Slice end, out NodeHeader* current)
        {
            int minFreeSpace = _minimumFreePagesInSectionSet ?
                _minimumFreePagesInSection :
                Math.Min(256, _lastTransactionPageUsage);

            current = null;
            int currentMax = 0;
			using (var it = _env.State.FreeSpaceRoot.Iterate(tx))
            {
                it.RequiredPrefix = _sectionsPrefix;
                it.MaxKey = end;
                if (it.Seek(start) == false)
                    return false;

                int triesAfterFindingSuitable = 256;
                do
                {
                    if (_recordsToSkip.Exists(x => x.Compare(it.CurrentKey, _env.SliceComparer) == 0))
                        continue; // if it is marked in memory for either update / delete, we don't want it

                    if (current != null)
                        triesAfterFindingSuitable--;

                    if (currentKey != null)
                    {
                        if (_currentKey.Compare(it.CurrentKey, _env.SliceComparer) == 0)
                            continue; // skip current one
                    }

                    using (var stream = it.CreateStreamForCurrent())
                    using (var reader = new BinaryReader(stream))
                    {
                        stream.Position = sizeof(long);
                        var largestSeq = reader.ReadInt32();
                        if (largestSeq < minSeq)
                            continue;

                        var pageCount = reader.ReadInt32();

                        if (pageCount < minFreeSpace || pageCount < currentMax)
                            continue;


                        current = it.Current;
                        currentMax = pageCount;
                    }

                } while (it.MoveNext() && triesAfterFindingSuitable >= 0);
            }
            return current != null;
        }

        public void UpdateSections(Transaction tx, long oldestTx)
        {
            var sections = new Dictionary<long, Section>();

            if (_current != null)
                sections[_current.Id] = _current;

            while (_freedTransactions.First != null && _freedTransactions.First.Value.Id < oldestTx)
            {
                var val = _freedTransactions.First.Value;
                _freedTransactions.RemoveFirst();
                _recordsToDelete.Add(val.Key);
                _recordsToSkip.Add(val.Key);
                foreach (var page in val.Pages)
                {
                    var sectionId = page / 1024;
                    Section section;
                    if (sections.TryGetValue(sectionId, out section) == false)
                    {
                        sections[sectionId] = section = LoadSection(tx, sectionId);
                    }

                    if (section == _current)
                    {
                        _currentChanged = true;
                    }

                    section.Sequences.Add(page);
                }
            }

            foreach (var section in sections.Values)
            {
                if (section == _current)
                    continue; // persisted at the end of the transaction, anyway
                WriteSection(tx, section);
            }
        }

        private void WriteSection(Transaction tx, Section section)
        {
            var size = sizeof(long) +  // id
                sizeof(int) + // largest sequence
                 sizeof(int) + // page count
                 sizeof(long) * section.Sequences.Count;

            _writingFreSpace = true;
            try
            {

                // we have to do it this way because the act of writing the free space
                // may change it, so we first allocate it (and deal with all the changes)
                // then we write the values

				var ptr = _env.State.FreeSpaceRoot.DirectAdd(tx, section.Key, size);
                using (var ums = new UnmanagedMemoryStream(ptr, size, size, FileAccess.ReadWrite))
                using (var writer = new BinaryWriter(ums))
                {
                    writer.Write(section.Id);
                    writer.Write(section.Sequences.LargestSequence);
                    writer.Write(section.Sequences.Count);
                    foreach (var page in section.Sequences)
                    {
                        writer.Write(page);
                    }
                }
            }
            finally
            {
                _writingFreSpace = false;
            }
        }

        private Section LoadSection(Transaction tx, long sectionId)
        {
            Slice key = string.Format("sect/{0:0000000000000000000}", sectionId);

            var section = new Section
            {
                Key = key,
                Id = sectionId,
                Sequences = new ConsecutiveSequences()
            };

			var read = _env.State.FreeSpaceRoot.Read(tx, key);
			if (read == null)
				return section;

            using (var stream = read.Stream)
            {
                var savedSectionId = ReadSectionPages(stream, section);
                Debug.Assert(savedSectionId == sectionId);
                return section;
            }
        }

        private static long ReadSectionPages(Stream stream, Section section)
        {
            using (var reader = new BinaryReader(stream))
            {
                var savedSectionId = reader.ReadInt64();
                var largestSequence = reader.ReadInt32();
                var pageCount = reader.ReadInt32();
                for (int i = 0; i < pageCount; i++)
                {
                    section.Sequences.Add(reader.ReadInt64());
                }
                Debug.Assert(largestSequence == section.Sequences.LargestSequence);
                return savedSectionId;
            }
        }

        public void Restore(Transaction tx)
        {
            // read all the freed transactions to memory, we expect this
            // number to be pretty small, so we don't worry about holding it all 
            // in memory
			using (var it = _env.State.FreeSpaceRoot.Iterate(tx))
            {
                it.RequiredPrefix = _txPrefix;

                if (it.Seek(_txPrefix) == false)
                    return;

                do
                {
                    var freedTransaction = new FreedTransaction
                        {
                            Key = it.CurrentKey.Clone(),
                            Pages = new List<long>()
                        };

                    var size = it.GetCurrentDataSize() / sizeof(long);

                    Debug.Assert(size > 1);


                    using (var stream = it.CreateStreamForCurrent())
                    using (var reader = new BinaryReader(stream))
                    {
                        freedTransaction.Id = reader.ReadInt64();
                        for (var i = 1; i < size; i++)
                        {
                            freedTransaction.Pages.Add(reader.ReadInt64());
                        }
                    }

                    _freedTransactions.AddLast(freedTransaction);
                } while (it.MoveNext());
            }
        }

        public void LastTransactionPageUsage(int pages)
        {
            if (_lastTransactionPageUsage < pages)
            {
                // to register a drop, we need to see a 25% reduction in the size
                if (pages - _lastTransactionPageUsage < _lastTransactionPageUsage / 4)
                    return;
            }

            _lastTransactionPageUsage = pages;
        }

        public void RegisterFreePages(Slice key, long id, List<long> freedPages)
        {
            _freedTransactions.AddLast(new FreedTransaction
                {
                    Key = key,
                    Id = id,
                    Pages = new List<long>(freedPages)
                });
        }

        public int MinimumFreePagesInSection
        {
            get { return _minimumFreePagesInSection; }
            set
            {
                _minimumFreePagesInSectionSet = true;
                _minimumFreePagesInSection = value;
            }
        }

        public void FlushFreeState(Transaction tx)
        {
            while (_recordsToDelete.Count > 0)
            {
                var local = new List<Slice>(_recordsToDelete);
                _recordsToDelete.Clear();

                foreach (var slice in local)
                {
					_env.State.FreeSpaceRoot.Delete(tx, slice);
                }
            }

            while (_recordsToUpdate.Count > 0)
            {
                // saving this may generate more records to update
                var local = new List<Section>(_recordsToUpdate);
                _recordsToUpdate.Clear();

                foreach (var section in local)
                {
                    WriteSection(tx, section);
                }
            }

            // the act of flushing the current section
            // may cause us to move to another section
            // need to handle this scenario
            while (true)
            {
                if (_current == null || _currentChanged == false)
                {
                    _recordsToSkip.Clear();
                    return;
                }
                if (_current.Sequences.Count == 0)
                {
                    Debug.Assert(_currentKey != null);
                    var old = _currentKey;
                    _currentKey = null;
                    _currentChanged = false;
                    _current = null; // make sure that we don't allocate from this while deleting it
                    
                    _writingFreSpace = true; //we don't want to recurse into free space while deleting the empty free space
                    try
                    {
						_env.State.FreeSpaceRoot.Delete(tx, old);
                    }
                    finally
                    {
                        _writingFreSpace = false;
                    }
                    return;
                }
                WriteSection(tx, _current);
                _currentChanged = false;
            }

        }

        public List<long> AllPages(Transaction tx)
        {
            var results = new List<long>();

            foreach (var freedTransaction in _freedTransactions)
            {
                results.AddRange(freedTransaction.Pages);
            }

            if (_current != null)
                results.AddRange(_current.Sequences);

			using (var it = _env.State.FreeSpaceRoot.Iterate(tx))
            {
                it.RequiredPrefix = _sectionsPrefix;

                if (it.Seek(_sectionsPrefix) == false)
                    return results;

                do
                {
                    if (_currentKey != null && _currentKey.Compare(it.CurrentKey, _env.SliceComparer) == 0)
                        continue;
                    using (var stream = it.CreateStreamForCurrent())
                    {
                        var section = new Section();
                        ReadSectionPages(stream, section);
                        results.AddRange(section.Sequences);
                    }
                } while (it.MoveNext());
            }

            return results;
        }
    }
}
