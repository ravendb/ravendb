// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public class SubscriptionCriteria
    {
        public string KeyStartsWith { get; set; }
        public long? StartEtag { get; set; }

        public string[] BelongsToAnyCollection { get; set; }

        public Dictionary<string, RavenJToken> PropertiesMatch { get; set; }

        public Dictionary<string, RavenJToken> PropertiesNotMatch { get; set; } 
    }

    public class SubscriptionCriteria<T>
    {
        public SubscriptionCriteria()
        {
            PropertiesMatch = new Dictionary<Expression<Func<T, object>>, RavenJToken>();
            PropertiesNotMatch = new Dictionary<Expression<Func<T, object>>, RavenJToken>();
        }

        public string KeyStartsWith { get; set; }
        public long? StartEtag { get; set; }

        public IDictionary<Expression<Func<T, object>>, RavenJToken> PropertiesMatch { get; set; }

        public IDictionary<Expression<Func<T, object>>, RavenJToken> PropertiesNotMatch { get; set; }

        public void PropertyMatch(Expression<Func<T, object>> field, RavenJToken indexing)
        {
            PropertiesMatch.Add(field, indexing);
        }

        public void PropertyNotMatch(Expression<Func<T, object>> field, RavenJToken indexing)
        {
            PropertiesNotMatch.Add(field, indexing);
        }

        public Dictionary<string, RavenJToken> GetPropertiesMatchStrings()
        {
            return ConvertToStringDictionary(PropertiesMatch);
        }

        public Dictionary<string, RavenJToken> GetPropertiesNotMatchStrings()
        {
            return ConvertToStringDictionary(PropertiesNotMatch);
        }

        private Dictionary<string, TValue> ConvertToStringDictionary<TValue>(IEnumerable<KeyValuePair<Expression<Func<T, object>>, TValue>> input)
        {
            var result = new Dictionary<string, TValue>();
            foreach (var value in input)
            {
                var propertyPath = value.Key.ToPropertyPath();
                result[propertyPath] = value.Value;
            }
            return result;
        }
    }
}
