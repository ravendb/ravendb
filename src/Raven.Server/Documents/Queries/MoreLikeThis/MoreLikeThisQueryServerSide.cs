using System;
using System.Collections.Generic;
using System.Globalization;
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
        public static MoreLikeThisQueryServerSide Create(BlittableJsonReaderObject json)
        {
            var result = new MoreLikeThisQueryServerSide();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            foreach (var propertyIndex in json.GetPropertiesByInsertionOrder())
            {
                json.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                switch (propertyDetails.Name)
                {
                    case nameof(MinimumWordLength):
                        if (propertyDetails.Value != null)
                            result.MinimumWordLength = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(MinimumTermFrequency):
                        if (propertyDetails.Value != null)
                            result.MinimumTermFrequency = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(MinimumDocumentFrequency):
                        if (propertyDetails.Value != null)
                            result.MinimumDocumentFrequency = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(MaximumDocumentFrequency):
                        if (propertyDetails.Value != null)
                            result.MaximumDocumentFrequency = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(MaximumQueryTerms):
                        if (propertyDetails.Value != null)
                            result.MaximumQueryTerms = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(MaximumNumberOfTokensParsed):
                        if (propertyDetails.Value != null)
                            result.MaximumNumberOfTokensParsed = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(MaximumDocumentFrequencyPercentage):
                        if (propertyDetails.Value != null)
                            result.MaximumDocumentFrequencyPercentage = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(StopWordsDocumentId):
                            result.StopWordsDocumentId = propertyDetails.Value?.ToString();
                        break;
                    case nameof(IndexName):
                        result.IndexName = propertyDetails.Value?.ToString();
                        break;
                    case nameof(DocumentId):
                        result.DocumentId = propertyDetails.Value?.ToString();
                        break;
                    case nameof(PageSize):
                        result.PageSize = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(Transformer):
                        result.Transformer = propertyDetails.Value?.ToString();
                        break;
                    case nameof(AdditionalQuery):
                        result.AdditionalQuery = propertyDetails.Value?.ToString();
                        break;
                    case nameof(Boost):
                        if (propertyDetails.Value != null)
                            result.Boost = (bool)propertyDetails.Value;
                        break;
                    case nameof(BoostFactor):
                        if (propertyDetails.Value != null)
                            result.BoostFactor = ((LazyNumberValue)propertyDetails.Value).ToSingle(CultureInfo.InvariantCulture);
                        break;
                    case nameof(Includes):
                        var includesArray = propertyDetails.Value as BlittableJsonReaderArray;
                        if (includesArray == null || includesArray.Length == 0)
                            continue;

                        result.Includes = new string[includesArray.Length];
                        for (var i = 0; i < includesArray.Length; i++)
                            result.Includes[i] = includesArray.GetStringByIndex(i);
                        break;
                    case nameof(Fields):
                        var fieldsArray = propertyDetails.Value as BlittableJsonReaderArray;
                        if (fieldsArray == null || fieldsArray.Length == 0)
                            continue;

                        result.Fields = new string[fieldsArray.Length];
                        for (var i = 0; i < fieldsArray.Length; i++)
                            result.Fields[i] = fieldsArray.GetStringByIndex(i);
                        break;
                    case nameof(TransformerParameters):
                        result.TransformerParameters = (BlittableJsonReaderObject)propertyDetails.Value;
                        break;
                }
            }

            return result;
        }

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
    }
}