using System;
using Lucene.Net.Search;

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

		public SortField ToLuceneSortField()
		{
			return new SortField(Field, Descending);
		}
	}
}