using System;
using System.Collections.Generic;
using Voron.Data.BTrees;
using Voron.Impl.Journal;

namespace Voron.Impl
{
    public class StorageEnvironmentState
    {
        public TreeMutableState Root;
        public StorageEnvironmentOptions Options;
        
        public long TransactionCounter;
        public List<JournalSnapshot> SnapshotCache;
        public Dictionary<int, PagerState> PagerStatesAllScratchesCache;

        public long NextPageNumber;

        private StorageEnvironmentState()
        {
        }

        public StorageEnvironmentState(Tree root, long nextPageNumber)
        {
            if (root != null)
                Root = root.State;
            NextPageNumber = nextPageNumber;
            SnapshotCache = new List<JournalSnapshot>();
            PagerStatesAllScratchesCache = new Dictionary<int, PagerState>();
        }

        public StorageEnvironmentState Clone()
        {
            return new StorageEnvironmentState
                {
                    Root = Root?.Clone(),
                    NextPageNumber = NextPageNumber,
                    Options = Options,
                    SnapshotCache = SnapshotCache,
                    TransactionCounter = TransactionCounter,
                    PagerStatesAllScratchesCache = PagerStatesAllScratchesCache
                };
        }
    }
}
