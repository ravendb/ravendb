namespace Raven.Studio.Features.Query
{
	public class FieldAndTerm
	{
		public FieldAndTerm(string field, string term)
		{
			Field = field;
			Term = term;
		}

		public string Field { get; set; }
		public string Term { get; set; }
		public string SuggestedTerm { get; set; }
	}
}