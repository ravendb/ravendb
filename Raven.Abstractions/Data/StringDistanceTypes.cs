namespace Raven.Abstractions.Data
{
    /// <summary>
    /// String distance algorithms used in suggestion query
    /// </summary>
    public enum StringDistanceTypes
    {
        /// <summary>
        /// Default, equivalent to Levenshtein
        /// </summary>
        Default = 0,
        /// <summary>
        /// JaroWinkler distance algorithm
        /// </summary>
        JaroWinkler,
        /// <summary>
        /// Levenshtein distance algorithm (default)
        /// </summary>
        Levenshtein,
        /// <summary>
        /// NGram distance algorithm
        /// </summary>
        NGram,
    }
}