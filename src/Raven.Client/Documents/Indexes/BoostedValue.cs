namespace Raven.Client.Documents.Indexes
{
    public class BoostedValue
    {
        /// <summary>
        /// Boost factor.
        /// </summary>
        public float Boost { get; set; }

        /// <summary>
        /// Boosted value.
        /// </summary>
        public object Value { get; set; }
    }
}
