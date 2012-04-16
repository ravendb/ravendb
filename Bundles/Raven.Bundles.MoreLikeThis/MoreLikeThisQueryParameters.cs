using System;
using System.Collections.Specialized;
using System.Text;
using System.Text.RegularExpressions;

#if CLIENT
namespace Raven.Client.MoreLikeThis
#else
namespace Raven.Bundles.MoreLikeThis
#endif
{
	public class MoreLikeThisQueryParameters
	{
		public MoreLikeThisQueryParameters()
		{
			MapGroupFields = new StringDictionary();
		}

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
		/// Values for the the mapping group fields to use as the basis for comparison
		/// </summary>
		public StringDictionary MapGroupFields { get; set; }

		public string GetRequestUri(string index)
		{
			var uri = new StringBuilder();

			string pathSuffix = "";

			if (MapGroupFields.Count > 0)
			{
				var separator = "";
				foreach(string key in MapGroupFields.Keys)
				{
					pathSuffix = pathSuffix + separator + Uri.EscapeUriString(key) + '=' + Uri.EscapeUriString(MapGroupFields[key]);
					separator = ";";
				}
			}
			else
			{
				pathSuffix = Uri.EscapeUriString(DocumentId);
			}

			uri.AppendFormat("/morelikethis/{0}/{1}?", Uri.EscapeUriString(index), pathSuffix);
			if (Fields != null)
			{
				foreach (var field in Fields)
				{
					uri.AppendFormat("fields={0}&", field);
				}
			}
			if (Boost != null && Boost != DefaultBoost)
				uri.Append("boost=true&");
			if (MaximumQueryTerms != null && MaximumQueryTerms != DefaultMaximumQueryTerms)
				uri.AppendFormat("maxQueryTerms={0}&", MaximumQueryTerms);
			if (MaximumNumberOfTokensParsed != null && MaximumNumberOfTokensParsed != DefaultMaximumNumberOfTokensParsed)
				uri.AppendFormat("maxNumTokens={0}&", MaximumNumberOfTokensParsed);
			if (MaximumWordLength != null && MaximumWordLength != DefaultMaximumWordLength)
				uri.AppendFormat("maxWordLen={0}&", MaximumWordLength);
			if (MinimumDocumentFrequency != null && MinimumDocumentFrequency != DefaltMinimumDocumentFrequency)
				uri.AppendFormat("minDocFreq={0}&", MinimumDocumentFrequency);
			if (MinimumTermFrequency != null && MinimumTermFrequency != DefaultMinimumTermFrequency)
				uri.AppendFormat("minTermFreq={0}&", MinimumTermFrequency);
			if (MinimumWordLength != null && MinimumWordLength != DefaultMinimumWordLength)
				uri.AppendFormat("minWordLen={0}&", MinimumWordLength);
			if (StopWordsDocumentId != null)
				uri.AppendFormat("stopWords={0}&", StopWordsDocumentId);
			return uri.ToString();
		}

#if !CLIENT
		public static MoreLikeThisQueryParameters GetParametersFromPath(Regex urlMatcher, string path, NameValueCollection query, out string indexName)
		{
			var match = urlMatcher.Match(path);

			indexName = match.Groups[1].Value;

			var results = new MoreLikeThisQueryParameters
			{
				Fields = query.GetValues("fields"),
				Boost = query.Get("boost").ToNullableBool(),
				MaximumNumberOfTokensParsed = query.Get("maxNumTokens").ToNullableInt(),
				MaximumQueryTerms = query.Get("maxQueryTerms").ToNullableInt(),
				MaximumWordLength = query.Get("maxWordLen").ToNullableInt(),
				MinimumDocumentFrequency = query.Get("minDocFreq").ToNullableInt(),
				MinimumTermFrequency = query.Get("minTermFreq").ToNullableInt(),
				MinimumWordLength = query.Get("minWordLen").ToNullableInt(),
				StopWordsDocumentId = query.Get("stopWords"),
			};

			var keyValues = match.Groups[2].Value.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

			foreach(var keyValue in keyValues)
			{
				var split = keyValue.IndexOf('=');

				if (split >= 0)
				{
					results.MapGroupFields.Add(keyValue.Substring(0, split), keyValue.Substring(split+1));
				} 
				else
				{
					results.DocumentId = keyValue;    
				}
			}

			indexName = match.Groups[1].Value;
			return results;
		}

		public static MoreLikeThisQueryParameters GetParametersFromPath(string pathAndQuery, out string indexName)
		{
			var path = pathAndQuery;
			var query = "";

			var split = pathAndQuery.IndexOf('?');
			if (split >= 0)
			{
				path = pathAndQuery.Substring(0, split);
				query = pathAndQuery.Substring(split);
			}

			return GetParametersFromPath(
				new Regex(new MoreLikeThisResponder().UrlPattern), 
				path, 
				System.Web.HttpUtility.ParseQueryString(query), 
				out indexName);
		}
#endif
	}
}
