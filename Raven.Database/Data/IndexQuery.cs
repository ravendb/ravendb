using System;
using Raven.Database.Extensions;

namespace Raven.Database.Data
{
	public class IndexQuery
	{
		public IndexQuery()
		{
			TotalSize = new Reference<int>();
		    PageSize = 128;
		}

		public string Query { get;  set; }

		public Reference<int> TotalSize { get;  private set; }

		public int Start { get;  set; }

		public int PageSize { get;  set; }

		public string[] FieldsToFetch { get; set; }

		public SortedField[] SortedFields { get; set; }

        public DateTime? Cutoff { get; set; }
	}
}