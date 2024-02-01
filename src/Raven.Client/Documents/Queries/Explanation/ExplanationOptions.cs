namespace Raven.Client.Documents.Queries.Explanation
{
    /// <summary>
    /// Additional configuration to explanation query. 
    /// </summary>
    public sealed class ExplanationOptions
    {
        /// <summary>
        /// Scope explanation to specific group by key.
        /// </summary>
        public string GroupKey { get; set; }
    }
}
