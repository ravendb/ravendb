using System;
using System.Globalization;
using Raven.Database.Indexing;

namespace Raven.Database.Data
{
	public class SortedField
	{
		public SortedField(string fieldWithPotentialPrefix)
		{
			if(fieldWithPotentialPrefix.StartsWith("+"))
			{
				Field = fieldWithPotentialPrefix.Substring(1);
			}
			else if (fieldWithPotentialPrefix.StartsWith("-"))
			{
				Field = fieldWithPotentialPrefix.Substring(1);
				Descending = true;
			}
			else
			{
				Field = fieldWithPotentialPrefix;
			}
		}

		public string Field { get; set; }
		public bool Descending { get; set; }
#if !CLIENT
		[CLSCompliant(false)]
		public Lucene.Net.Search.SortField ToLuceneSortField(IndexDefinition definition)
		{
			var sortOptions = definition.GetSortOption(Field);
			if(sortOptions == null)
				return new  Lucene.Net.Search.SortField(Field, CultureInfo.InvariantCulture, Descending);
			return new Lucene.Net.Search.SortField(Field, (int)sortOptions.Value, Descending);
		}
#endif
	}
}