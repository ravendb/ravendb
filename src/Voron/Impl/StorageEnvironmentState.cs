using System.Diagnostics;
using Voron.Data.BTrees;
using Voron.Global;

namespace Voron.Impl
{
    public sealed class StorageEnvironmentState
    {
        public TreeMutableState Root { get; private set; }

        public long NextPageNumber { get; private set; }

        private StorageEnvironmentState(TreeMutableState root, long nextPageNumber)
        {
            Root = root;
            NextPageNumber = nextPageNumber;
        }

        public StorageEnvironmentState(long nextPage)
        {
            NextPageNumber = nextPage;
        }

        public void Initialize(TreeMutableState rootObjectsState)
        {
            Root = rootObjectsState;
        }

        public void UpdateNextPage(long nextPage)
        {
            NextPageNumber = nextPage;
        }

        public StorageEnvironmentState Clone()
        {
            return new StorageEnvironmentState(Root?.Clone(), NextPageNumber);
        }
    }
}
