using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Voron.Data.BTrees;
using Voron.Impl.Journal;

namespace Voron.Impl
{
    public class StorageEnvironmentState
    {
        public TreeMutableState Root;
        public StorageEnvironmentOptions Options;
        
        public long TransactionCounter;
        public ReadOnlyCollection<JournalSnapshot> SnapshotCache;
        public ReadOnlyDictionary<int, PagerState> PagerStatesAllScratchesCache;
        public object ExternalState;

        public long NextPageNumber;

        private StorageEnvironmentState()
        {
        }

        public StorageEnvironmentState(Tree root, long nextPageNumber)
        {
            if (root != null)
                Root = root.State;
            NextPageNumber = nextPageNumber;
            SnapshotCache = new List<JournalSnapshot>().AsReadOnly();
            PagerStatesAllScratchesCache = new ReadOnlyDictionary<int, PagerState>(new Dictionary<int, PagerState>());
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
                PagerStatesAllScratchesCache = PagerStatesAllScratchesCache,
                ExternalState = ExternalState
            };
        }
    }
}
