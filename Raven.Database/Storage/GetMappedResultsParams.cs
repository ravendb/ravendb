using Raven.Database.Indexing;

namespace Raven.Database.Storage
{
	public class GetMappedResultsParams
	{
		private readonly string view;
		private readonly string reduceKey;
		private byte[] viewAndReduceKeyHashed;

		public GetMappedResultsParams(string view, string reduceKey)
		{
			this.view = view;
			this.reduceKey = reduceKey;
		}

		public string View
		{
			get { return view; }
		}

		public string ReduceKey
		{
			get { return reduceKey; }
		}

		public byte[] ViewAndReduceKeyHashed
		{
			get
			{
				if (viewAndReduceKeyHashed == null)
				{
					viewAndReduceKeyHashed = MapReduceIndex.ComputeHash(view, reduceKey);
				}
				return viewAndReduceKeyHashed;
			}
		}
	}
}