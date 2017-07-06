using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestion;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Suggestions
{
    public class SuggestionsQueryServerSide : IIndexQuery
    {
        public static float DefaultAccuracy = 0.5f;

        public static int DefaultMaxSuggestions = 15;

        public static StringDistanceTypes DefaultDistance = StringDistanceTypes.Levenshtein;

        /// <summary>
        /// Create a new instance of <seealso cref="SuggestionQuery"/>
        /// </summary>
        public SuggestionsQueryServerSide()
        {
            Accuracy = 0.5f;
            Distance = DefaultDistance;
            MaxSuggestions = DefaultMaxSuggestions;
        }


        public static SuggestionsQueryServerSide Create(HttpContext httpContext, int pageSize, JsonOperationContext context)
        {
            var result = new SuggestionsQueryServerSide
            {
                // all defaults which need to have custom value
                PageSize = pageSize,                
            };

            foreach (var item in httpContext.Request.Query)
            {
                // Read the requests data from the query string.                
                try
                {
                    if (string.Equals(item.Key, "distance", StringComparison.OrdinalIgnoreCase))
                    {
                        if (Enum.TryParse(item.Value[0], true, out StringDistanceTypes distance))
                            result.Distance = distance;
                    }
                    else if (string.Equals(item.Key, "accuracy", StringComparison.OrdinalIgnoreCase))
                    {
                        if (float.TryParse(item.Value[0], out float value))
                            result.Accuracy = value;
                    }
                    else if (string.Equals(item.Key, "maxSuggestions", StringComparison.OrdinalIgnoreCase))
                    {
                        if (int.TryParse(item.Value[0], out int maxSuggestions))
                            result.MaxSuggestions = maxSuggestions;
                    }
                    else if (string.Equals(item.Key, "terms", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Term = item.Value[0];
                    }
                    else if (string.Equals(item.Key, "field", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Field = item.Value[0];
                    }
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                }
            }

            return result;
        }

        private int _pageSize;
        
        /// <summary>
        /// Maximum number of records that will be retrieved.
        /// </summary>
        public int PageSize
        {
            get => _pageSize;
            set
            {
                _pageSize = value;
                PageSizeSet = true;
            }
        }

        /// <summary>
        /// Whether the page size was explicitly set or still at its default value
        /// </summary>
        protected internal bool PageSizeSet { get; private set; }

        /// <summary>
        /// Term is what the user likely entered, and will used as the basis of the suggestions.
        /// </summary>
        public string Term { get; set; }

        /// <summary>
        /// Field to be used in conjunction with the index.
        /// </summary>
        public string Field { get; set; }

        /// <summary>
        /// Maximum number of suggestions to return.
        /// <para>Value:</para>
        /// <para>Default value is 15.</para>
        /// </summary>
        /// <value>Default value is 15.</value>
        public int MaxSuggestions { get; set; }

        /// <summary>
        /// String distance algorithm to use. If <c>null</c> then default algorithm is used (Levenshtein).
        /// </summary>
        public StringDistanceTypes Distance { get; set; }

        /// <summary>
        /// Suggestion accuracy. If <c>null</c> then default accuracy is used (0.5f).
        /// </summary>
        public float Accuracy { get; set; }

        /// <summary>
        /// Whatever to return the terms in order of popularity
        /// </summary>
        public bool Popularity { get; set; }
    }
}
