using System;
using Voron.Trees;

namespace Voron.Impl
{
	public class StorageEnvironmentState
	{
		public TreeMutableState Root { get; set; }
        public TreeMutableState FreeSpaceRoot { get; set; }
	    public StorageEnvironmentOptions Options { get; set; }

	    public long NextPageNumber;

		public StorageEnvironmentState() { }

		public StorageEnvironmentState(Tree freeSpaceRoot, Tree root, long nextPageNumber)
		{
			FreeSpaceRoot = freeSpaceRoot.State;
			Root = root.State;
			NextPageNumber = nextPageNumber;
		}

		public StorageEnvironmentState Clone()
		{
			return new StorageEnvironmentState
				{
					Root = Root != null ? Root.Clone() : null,
					FreeSpaceRoot = FreeSpaceRoot != null ? FreeSpaceRoot.Clone() : null,
					NextPageNumber = NextPageNumber,
                    Options = Options
				};
		}
	}
}
