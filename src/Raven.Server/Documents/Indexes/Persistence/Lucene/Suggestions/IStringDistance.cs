namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions
{
    /// <summary>
    /// Interface for string distances.
    /// </summary>
    internal interface IStringDistance
    {
        /// <summary>
        /// Returns a float between 0 and 1 based on how similar the specified strings are to one another.  
        /// Returning a value of 1 means the specified strings are identical and 0 means the
        /// string are maximally different.
        /// </summary>
        /// <param name="s1">The first string.</param>
        /// <param name="s2">The second string.</param>
        /// <returns>a float between 0 and 1 based on how similar the specified strings are to one another.</returns>
        float GetDistance(string s1, string s2);
    }
}