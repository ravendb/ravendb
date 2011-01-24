namespace Raven.Database.Storage
{
	public class GetMappedResultsParams
	{
		private readonly string view;
		private readonly string reduceKey;
		private readonly byte[] viewAndReduceKeyHashed;

		public GetMappedResultsParams(string view, string reduceKey, byte[] viewAndReduceKeyHashed)
		{
			this.view = view;
			this.reduceKey = reduceKey;
			this.viewAndReduceKeyHashed = viewAndReduceKeyHashed;
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
			get { return viewAndReduceKeyHashed; }
		}
	}
}