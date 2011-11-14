namespace Raven.Bundles.MoreLikeThis
{
    public class MoreLikeThisQueryParameters
    {
        public const int DefaultMaximumNumberOfTokensParsed = 5000;
        public const int DefaultMinimumTermFrequency = 2;
        public const int DefaltMinimumDocumentFrequency = 5;
        public const bool DefaultBoost = false;
        public const int DefaultMinimumWordLength = 0;
        public const int DefaultMaximumWordLength = 0;
        public const int DefaultMaximumQueryTerms = 25;

        /// <summary>
        /// Ignore terms with less than this frequency in the source doc. Default is 2.
        /// </summary>
        public int? MinimumTermFrequency { get; set; }

        /// <summary>
        /// Ignore words which do not occur in at least this many documents. Default is 5.
        /// </summary>
        public int? MinimumDocumentFrequency { get; set; }

        /// <summary>
        /// Boost terms in query based on score. Default is false.
        /// </summary>
        public bool? Boost { get; set; }

        /// <summary>
        /// Ignore words less than this length or if 0 then this has no effect. Default is 0.
        /// </summary>
        public int? MinimumWordLength { get; set; }

        /// <summary>
        /// Ignore words greater than this length or if 0 then this has no effect. Default is 0.
        /// </summary>
        public int? MaximumWordLength { get; set; }

        /// <summary>
        /// Return a Query with no more than this many terms. Default is 25.
        /// </summary> 
        public int? MaximumQueryTerms { get; set; }

        /// <summary>
        /// The maximum number of tokens to parse in each example doc field that is not stored with TermVector support. Default is 5000.
        /// </summary>
        public int? MaximumNumberOfTokensParsed { get; set; }

        public string StopWordsDocumentId { get; set; }
    }
}
