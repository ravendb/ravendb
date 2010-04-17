using Raven.Database.Extensions;

namespace Raven.Database.Data
{
	public class IndexQuery
	{
		public IndexQuery(string query, int start, int pageSize)
		{
			Query = query;
			Start = start;
			PageSize = pageSize;
			TotalSize = new Reference<int>();
		}

		public string Query { get; private set; }

		public Reference<int> TotalSize { get; private set; }

		public int Start { get; private set; }

		public int PageSize { get; private set; }

		public string[] FieldsToFetch { get; set; }

		public SortedField[] SortedFields { get; set; }
	}
}