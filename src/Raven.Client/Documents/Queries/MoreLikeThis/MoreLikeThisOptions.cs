namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisOptions
    {
        public static MoreLikeThisOptions Default = new MoreLikeThisOptions();

        /// <summary>
        /// Ignore terms with less than this frequency in the source doc. Default is 2.
        /// </summary>
        public int? MinimumTermFrequency { get; set; }

        /// <summary>
        /// Ignore words which do not occur in at least this many documents. Default is 5.
        /// </summary>
        public int? MinimumDocumentFrequency { get; set; }

        /// <summary>
        /// The fields to compare
        /// </summary>
        public string[] Fields { get; set; }

        /// <summary>
        /// Ignore words less than this length or if 0 then this has no effect. Default is 0.
        /// </summary>
        public int? MinimumWordLength { get; set; }

        /// <summary>
        /// Boost terms in query based on score. Default is false.
        /// </summary>
        public bool? Boost { get; set; }

        /// <summary>
        /// Boost factor when boosting based on score. Default is 1.
        /// </summary>
        public float? BoostFactor { get; set; }

        /// <summary>
        /// The document id containing the custom stop words
        /// </summary>
        public string StopWordsDocumentId { get; set; }
    }
}
