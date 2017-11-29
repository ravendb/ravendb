namespace Raven.Client.Documents.Queries.Suggestions
{
    public class SuggestionOptions
    {
        internal static readonly SuggestionOptions Default = new SuggestionOptions();

        public static readonly float DefaultAccuracy = 0.5f;

        public static readonly int DefaultPageSize = 15;

        public static readonly StringDistanceTypes DefaultDistance = StringDistanceTypes.Levenshtein;

        public static readonly SuggestionSortMode DefaultSortMode = SuggestionSortMode.Popularity;

        public SuggestionOptions()
        {
            SortMode = DefaultSortMode;
            Distance = DefaultDistance;
            Accuracy = DefaultAccuracy;
            PageSize = DefaultPageSize;
        }

        public int PageSize { get; set; }

        /// <summary>
        /// String distance algorithm to use. If <c>null</c> then default algorithm is used (Levenshtein).
        /// </summary>
        public StringDistanceTypes? Distance { get; set; }

        /// <summary>
        /// Suggestion accuracy. If <c>null</c> then default accuracy is used (0.5f).
        /// </summary>
        public float? Accuracy { get; set; }

        /// <summary>
        /// Whether to return the terms in order of popularity
        /// </summary>
        public SuggestionSortMode SortMode { get; set; }
    }
}
