namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetOptions
    {
        public static FacetOptions Default = new FacetOptions();

        /// <summary>
        /// Indicates how terms should be sorted.
        /// </summary>
        /// <value>FacetTermSortMode.ValueAsc by default.</value>
        public FacetTermSortMode TermSortMode { get; set; }

        /// <summary>
        /// Indicates if remaining terms should be included in results.
        /// </summary>
        public bool IncludeRemainingTerms { get; set; }
    }
}
