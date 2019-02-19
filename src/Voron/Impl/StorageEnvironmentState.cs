using Voron.Data.BTrees;

namespace Voron.Impl
{
    public class StorageEnvironmentState
    {
        public TreeMutableState Root { get; set; }
        public StorageEnvironmentOptions Options { get; set; }

        public long NextPageNumber;

        public StorageEnvironmentState() { }

        public StorageEnvironmentState(Tree root, long nextPageNumber)
        {
            if (root != null)
                Root = root.State;
            NextPageNumber = nextPageNumber;
        }

        public StorageEnvironmentState Clone()
        {
            return new StorageEnvironmentState
                {
                    Root = Root?.Clone(),
                    NextPageNumber = NextPageNumber,
                    Options = Options
                };
        }
    }
}
