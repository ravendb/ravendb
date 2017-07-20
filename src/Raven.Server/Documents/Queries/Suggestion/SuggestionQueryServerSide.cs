using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestion;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Suggestion
{
    public class SuggestionQueryServerSide : SuggestionQuery, IIndexQuery
    {
        private int _pageSize;

        public int PageSize
        {
            get => _pageSize;
            set
            {
                _pageSize = value;
                PageSizeSet = true;
            }
        }

        protected internal bool PageSizeSet { get; private set; }

        public static SuggestionQueryServerSide Create(BlittableJsonReaderObject json)
        {
            var result = JsonDeserializationServer.SuggestionQuery(json);

            if (string.IsNullOrWhiteSpace(result.IndexName))
                throw new InvalidOperationException($"Index query does not contain '{nameof(IndexName)}' field.");

            result.ApplyDefaultValuesIfNecessary();
            return result;
        }

        public static SuggestionQueryServerSide Create(HttpContext httpContext, int pageSize, JsonOperationContext context)
        {
            if (httpContext.Request.Query.TryGetValue("index", out var index) == false || index.Count == 0 || string.IsNullOrWhiteSpace(index[0]))
                throw new InvalidOperationException("Missing mandatory query string parameter 'index'.");

            var result = new SuggestionQueryServerSide
            {
                IndexName = index[0]
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
                    else if (string.Equals(item.Key, "popular", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Popularity = bool.Parse(item.Value[0]);
                    }
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                }
            }

            result.ApplyDefaultValuesIfNecessary();
            return result;
        }

        private void ApplyDefaultValuesIfNecessary()
        {
            if (Accuracy.HasValue == false)
                Accuracy = DefaultAccuracy;

            if (Distance.HasValue == false)
                Distance = DefaultDistance;

            if (MaxSuggestions == 0)
                MaxSuggestions = DefaultMaxSuggestions;
        }
    }
}
