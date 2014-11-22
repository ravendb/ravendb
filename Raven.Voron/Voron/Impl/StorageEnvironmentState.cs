using Voron.Trees;

namespace Voron.Impl
{
    public class StorageEnvironmentState
    {
        public Tree Root { get; set; }
        public Tree FreeSpaceRoot { get; set; }

        public long NextPageNumber;

        public StorageEnvironmentState() { }

        public StorageEnvironmentState(Tree freeSpaceRoot, Tree root, long nextPageNumber)
        {
            FreeSpaceRoot = freeSpaceRoot;
            Root = root;
            NextPageNumber = nextPageNumber;
        }

        public StorageEnvironmentState Clone(Transaction tx)
        {
            return new StorageEnvironmentState
                {
					Root = Root != null ? Root.Clone(tx) : null,
					FreeSpaceRoot = FreeSpaceRoot != null ? FreeSpaceRoot.Clone(tx) : null,
                    NextPageNumber = NextPageNumber
                };
        }
    }
}
