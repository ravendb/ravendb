namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Defines type of aggregation for a field.
    /// </summary>
    public enum GroupByMethod
    {
        /// <summary>
        /// Each value from field will be treated separately. 
        /// </summary>
        None,

        /// <summary>
        /// Whole array is treated as single value (hash is calculated from whole array).
        /// </summary>
        Array
    }
}
