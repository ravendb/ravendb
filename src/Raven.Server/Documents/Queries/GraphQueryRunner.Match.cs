using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner
    {
        public unsafe struct Match // using struct because we have a single field 
        {
            private Dictionary<string, Document> _inner;

            public object Key => _inner;

            public int Count => _inner?.Count ?? 0;

            public IEnumerable<string> Aliases => _inner.Keys;

            public bool Empty => _inner == null || _inner.Count == 0;

            public Match(Match other)
            {
                if (other._inner == null)
                {
                    _inner = null;
                }
                else
                {
                    _inner = new Dictionary<string, Document>(other._inner);
                }
            }

            public Document Get(string alias)
            {
                Document result = null;
                _inner?.TryGetValue(alias, out result);
                if(result?.Id != null)
                {
                    result.EnsureMetadata();
                }
                return result;
            }           

            public bool TryGetAliasId(string alias, out long id)
            {
                id = -1;
                var hasKey = _inner.TryGetValue(alias, out var doc);

                if (hasKey)
                    id = (long)doc.Data.BasePointer;

                return hasKey;
            }

            public bool TryGetKey(string alias, out string key)
            {
                key = null;
                var hasKey = _inner.TryGetValue(alias, out var doc);

                if (hasKey)
                    key = doc.Id;

                return hasKey;
            }

            //try to set, but don't overwrite
            public long? TrySet(StringSegment alias, Document val)
            {
                if (_inner == null)
                    _inner = new Dictionary<string, Document>();

                if (_inner.TryAdd(alias, val) == false)
                    return null;
                return (long)val.Data.BasePointer;
            }

            public void Set(StringSegment alias, Document val)
            {
                if (_inner == null)
                    _inner = new Dictionary<string, Document>();

                _inner.Add(alias, val);
            }            

            public void PopulateVertices(DynamicJsonValue j)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    if (item.Value.Id != null)
                    {
                        item.Value.EnsureMetadata();
                    }
                    j[item.Key] = item.Value.Data;                    
                }
            }

            public void PopulateVertices(ref IntermediateResults i)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    i.Add(item.Key, this, item.Value);
                }
            }

            internal Document GetFirstResult()
            {
                foreach (var item in _inner)
                {
                    if (item.Value.Id != null)
                    {
                        item.Value.EnsureMetadata();
                    }

                    return item.Value;
                }
                throw new InvalidOperationException("Cannot return single result when there are no results");
            }

            internal Document GetResult(string alias)
            {
                var val = _inner[alias];
                if (val.Id != null)
                {
                    val.EnsureMetadata();
                }
                return val;
            }
        }
    }
}
