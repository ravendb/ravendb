namespace Raven.Client.Documents.Queries.Suggestions
{
    public class SuggestionWithTerm : SuggestionBase
    {
        public SuggestionWithTerm(string field)
            : base(field)
        {
        }

        public string Term { get; set; }

    }

    public class SuggestionWithTerms : SuggestionBase
    {
        public SuggestionWithTerms(string field)
            : base(field)
        {
        }

        public string[] Terms { get; set; }
    }

    public abstract class SuggestionBase
    {
        protected SuggestionBase(string field)
        {
            Field = field;
        }

        public string Field { get; set; }

        public string DisplayField { get; set; }

        public SuggestionOptions Options { get; set; }
    }
}
