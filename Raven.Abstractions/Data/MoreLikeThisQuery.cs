using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Specialized;

using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public class MoreLikeThisQuery
    {
        public MoreLikeThisQuery()
        {
            MapGroupFields = new NameValueCollection();
        }

        public const int DefaultMaximumNumberOfTokensParsed = 5000;
        public const int DefaultMinimumTermFrequency = 2;
        public const int DefaultMinimumDocumentFrequency = 5;
        public const int DefaultMaximumDocumentFrequency = int.MaxValue;
        public const bool DefaultBoost = false;
        public const float DefaultBoostFactor = 1;
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
        /// Ignore words which occur in more than this many documents. Default is Int32.MaxValue.
        /// </summary>
        public int? MaximumDocumentFrequency { get; set; }

        /// <summary>
        /// Ignore words which occur in more than this percentage of documents.
        /// </summary>
        public int? MaximumDocumentFrequencyPercentage { get; set; }


        /// <summary>
        /// Boost terms in query based on score. Default is false.
        /// </summary>
        public bool? Boost { get; set; }

        /// <summary>
        /// Boost factor when boosting based on score. Default is 1.
        /// </summary>
        public float? BoostFactor { get; set; }

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

        /// <summary>
        /// The document id containing the custom stop words
        /// </summary>
        public string StopWordsDocumentId { get; set; }

        /// <summary>
        /// The fields to compare
        /// </summary>
        public string[] Fields { get; set; }

        /// <summary>
        /// The document id to use as the basis for comparison
        /// </summary>
        public string DocumentId { get; set; }

        /// <summary>
        /// The name of the index to use for this operation
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// An additional query that the matching documents need to also
        /// match to be returned. 
        /// </summary>
        public string AdditionalQuery { get; set; }

        /// <summary>
        /// The default analyzer to be used for fields without any analyzer specified in the index definition. Default is LowerCaseKeywordAnalyzer.
        /// </summary>
        public string DefaultAnalyzerName { get; set; }

        /// <summary>
        /// An artificial document to use as the basis for comparison
        /// </summary>
        public string Document { get; set; }

        /// <summary>
        /// Values for the the mapping group fields to use as the basis for comparison
        /// </summary>
        public NameValueCollection MapGroupFields { get; set; }

        /// <summary>
        /// Transformer to use on the query results.
        /// </summary>
        public string ResultsTransformer { get; set; }

        /// <summary>
        /// Array of paths under which document Ids can be found. All found documents will be returned with the query results.
        /// </summary>
        public string[] Includes { get; set; }

        /// <summary>
        /// Parameters that will be passed to transformer.
        /// </summary>
        public Dictionary<string, RavenJToken> TransformerParameters { get; set; }

        public string GetRequestUri()
        {
            if (string.IsNullOrEmpty(IndexName))
                throw new InvalidOperationException("Index name cannot be null or empty");

            if (string.IsNullOrEmpty(DocumentId) && MapGroupFields.Count == 0 && string.IsNullOrEmpty(Document))
                throw new InvalidOperationException("The document id, map group fields or document are mandatory");

            var uri = new StringBuilder();
            uri.AppendFormat("/morelikethis/?index={0}&", Uri.EscapeUriString(IndexName));

            if (MapGroupFields.Count > 0)
            {
                var pathSuffix = string.Empty;
                var separator = string.Empty;
                foreach (string key in MapGroupFields.Keys)
                {
                    pathSuffix = pathSuffix + separator + key + '=' + MapGroupFields[key];
                    separator = ";";
                }

                uri.AppendFormat("docid={0}&", Uri.EscapeDataString(pathSuffix));
            }
            else if (!string.IsNullOrEmpty(DocumentId))
            {
                uri.AppendFormat("docid={0}&", Uri.EscapeDataString(DocumentId));
            }

            if (Fields != null)
            {
                foreach (var field in Fields)
                {
                    uri.AppendFormat("fields={0}&", field);
                }
            }
            if (string.IsNullOrWhiteSpace(AdditionalQuery) == false)
                uri.Append("query=").Append(Uri.EscapeDataString(AdditionalQuery)).Append("&");
            if (string.IsNullOrWhiteSpace(DefaultAnalyzerName) == false)
                uri.Append("defaultAnalyzer=").Append(Uri.EscapeDataString(DefaultAnalyzerName)).Append("&");
            if (string.IsNullOrWhiteSpace(Document) == false)
                uri.Append("document=").Append(Uri.EscapeDataString(Document)).Append("&");
            if (Boost != null && Boost != DefaultBoost)
                uri.Append("boost=true&");
            if (BoostFactor != null && BoostFactor != DefaultBoostFactor)
                uri.AppendFormat("boostFactor={0}&", BoostFactor);
            if (MaximumQueryTerms != null && MaximumQueryTerms != DefaultMaximumQueryTerms)
                uri.AppendFormat("maxQueryTerms={0}&", MaximumQueryTerms);
            if (MaximumNumberOfTokensParsed != null && MaximumNumberOfTokensParsed != DefaultMaximumNumberOfTokensParsed)
                uri.AppendFormat("maxNumTokens={0}&", MaximumNumberOfTokensParsed);
            if (MaximumWordLength != null && MaximumWordLength != DefaultMaximumWordLength)
                uri.AppendFormat("maxWordLen={0}&", MaximumWordLength);
            if (MinimumDocumentFrequency != null && MinimumDocumentFrequency != DefaultMinimumDocumentFrequency)
                uri.AppendFormat("minDocFreq={0}&", MinimumDocumentFrequency);
            if (MaximumDocumentFrequency != null && MaximumDocumentFrequency != DefaultMaximumDocumentFrequency)
                uri.AppendFormat("maxDocFreq={0}&", MaximumDocumentFrequency);
            if (MaximumDocumentFrequencyPercentage != null)
                uri.AppendFormat("maxDocFreqPct={0}&", MaximumDocumentFrequencyPercentage);
            if (MinimumTermFrequency != null && MinimumTermFrequency != DefaultMinimumTermFrequency)
                uri.AppendFormat("minTermFreq={0}&", MinimumTermFrequency);
            if (MinimumWordLength != null && MinimumWordLength != DefaultMinimumWordLength)
                uri.AppendFormat("minWordLen={0}&", MinimumWordLength);
            if (StopWordsDocumentId != null)
                uri.AppendFormat("stopWords={0}&", StopWordsDocumentId);
            if (string.IsNullOrEmpty(ResultsTransformer) == false)
                uri.AppendFormat("&resultsTransformer={0}", Uri.EscapeDataString(ResultsTransformer));

            if (TransformerParameters != null)
            {
                foreach (var input in TransformerParameters)
                {
                    uri.AppendFormat("&tp-{0}={1}", input.Key, input.Value);
                }
            }

            if (Includes != null && Includes.Length > 0)
                uri.Append(string.Join("&", Includes.Select(x => "include=" + x).ToArray()));

            return uri.ToString();
        }
    }
}
