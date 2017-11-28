namespace Raven.Client.Documents.Queries.Suggestion
{
    public class SuggestionOptions
    {
        public static SuggestionOptions Default = new SuggestionOptions();

        public static float DefaultAccuracy = 0.5f;

        public static int DefaultPageSize = 15;

        public static StringDistanceTypes DefaultDistance = StringDistanceTypes.Levenshtein;

        public SuggestionOptions()
        {
            Popularity = true;
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
        public bool Popularity { get; set; }
    }
}
