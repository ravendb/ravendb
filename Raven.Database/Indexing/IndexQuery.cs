using Raven.Database.Extensions;

namespace Raven.Database.Indexing
{
	public class IndexQuery
	{
		private readonly string query;
		private readonly Reference<int> totalSize;
		private readonly int start;
		private readonly int pageSize;
		private readonly string[] fieldsToFetch;

		public IndexQuery(string query, int start, int pageSize)
		{
			this.query = query;
			this.start = start;
			this.pageSize = pageSize;
			totalSize = new Reference<int>();
		}

		public IndexQuery(string query, int start, int pageSize, string[] fieldsToFetch)
		{
			this.query = query;
			totalSize = new Reference<int>();
			this.start = start;
			this.pageSize = pageSize;
			this.fieldsToFetch = fieldsToFetch;
		}

		public string Query
		{
			get { return query; }
		}

		public Reference<int> TotalSize
		{
			get { return totalSize; }
		}

		public int Start
		{
			get { return start; }
		}

		public int PageSize
		{
			get { return pageSize; }
		}

		public string[] FieldsToFetch
		{
			get { return fieldsToFetch; }
		}
	}
}