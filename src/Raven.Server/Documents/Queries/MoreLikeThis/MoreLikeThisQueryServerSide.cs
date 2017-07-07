using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Transformers;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.MoreLikeThis
{
    public sealed class MoreLikeThisQueryServerSide : MoreLikeThisQuery<BlittableJsonReaderObject>
    {
        public static MoreLikeThisQueryServerSide Create(HttpContext httpContext, int pageSize, JsonOperationContext context)
        {
            var result = new MoreLikeThisQueryServerSide
            {
                // all defaults which need to have custom value
                PageSize = pageSize
            };

            DynamicJsonValue transformerParameters = null;
            HashSet<string> includes = null;
            foreach (var item in httpContext.Request.Query)
            {
                try
                {
                    if (string.Equals(item.Key, "query", StringComparison.OrdinalIgnoreCase))
                    {
                        result.AdditionalQuery = item.Value[0];
                    }
                    else if (string.Equals(item.Key, "include", StringComparison.OrdinalIgnoreCase))
                    {
                        if (includes == null)
                            includes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                        includes.Add(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "transformer", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Transformer = item.Value[0];
                    }
                    else if (string.Equals(item.Key, "fields", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Fields = item.Value;
                    }
                    else if (string.Equals(item.Key, "boost", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Boost = bool.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "boostFactor", StringComparison.OrdinalIgnoreCase))
                    {
                        result.BoostFactor = float.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "maxNumTokens", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MaximumNumberOfTokensParsed = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "maxQueryTerms", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MaximumQueryTerms = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "maxWordLen", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MaximumWordLength = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "minDocFreq", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MinimumDocumentFrequency = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "maxDocFreq", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MaximumDocumentFrequency = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "maxDocFreqPct", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MaximumDocumentFrequencyPercentage = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "minTermFreq", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MinimumTermFrequency = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "minWordLen", StringComparison.OrdinalIgnoreCase))
                    {
                        result.MinimumWordLength = int.Parse(item.Value[0]);
                    }
                    else if (string.Equals(item.Key, "docId", StringComparison.OrdinalIgnoreCase))
                    {
                        result.DocumentId = item.Value[0];
                    }
                    else if (string.Equals(item.Key, "stopWords", StringComparison.OrdinalIgnoreCase))
                    {
                        result.StopWordsDocumentId = item.Value[0];
                    }
                    else
                    {
                        if (item.Key.StartsWith(TransformerParameter.Prefix, StringComparison.OrdinalIgnoreCase))
                        {
                            if (transformerParameters == null)
                                transformerParameters = new DynamicJsonValue();

                            transformerParameters[item.Key.Substring(TransformerParameter.Prefix.Length)] = item.Value[0];
                        }

                        if (item.Key.StartsWith("mgf-", StringComparison.OrdinalIgnoreCase))
                        {
                            result.MapGroupFields[item.Key.Substring(4)] = item.Value[0];
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                }
            }

            if (includes != null)
                result.Includes = includes.ToArray();

            if (transformerParameters != null)
                result.TransformerParameters = context.ReadObject(transformerParameters, "transformer/parameters");

            return result;
        }

        public string GetIndex()
        {
            throw new NotImplementedException("TODO arek");
        }
    }
}