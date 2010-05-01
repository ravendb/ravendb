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
		public  Lucene.Net.Search.SortField ToLuceneSortField()
		{
			return new  Lucene.Net.Search.SortField(Field, Descending);
		}
#endif
	}
}