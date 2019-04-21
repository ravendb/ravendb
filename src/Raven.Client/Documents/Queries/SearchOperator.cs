namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Search operator between terms in a search caluse
    /// </summary>
    public enum SearchOperator
    {
        /// <summary>
        /// Or operator will be used between all terms for a search clause, meaning a field value that matches any of the terms will be considered a match
        /// </summary>
        Or,
        /// <summary>
        /// And operator will be used between all terms for a search clause, meaning a field value matching all of the terms will be considered a match
        /// </summary>
        And
    }
}
