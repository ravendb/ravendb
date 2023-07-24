namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Search operator between terms in a search clause
    /// </summary>
    public enum SearchOperator
    {
        /// <summary>
        /// Or operator will be used between all terms of a search clause.
        /// A field value that matches any of the terms will be considered a match.
        /// </summary>
        Or,
        /// <summary>
        /// And operator will be used between all terms of a search clause.
        /// A field value matching all of the terms will be considered a match.
        /// </summary>
        And
    }
}
