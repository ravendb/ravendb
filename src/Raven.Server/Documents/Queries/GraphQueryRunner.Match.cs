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
        public struct Match // using struct because we have a single field 
        {
            private Dictionary<string, Document> _inner;

            public object Key => _inner;

            public IEnumerable<string> Aliases => _inner.Keys;

            public Document Get(string alias)
            {
                Document result = null;
                _inner?.TryGetValue(alias, out result);
                result?.EnsureMetadata();
                return result;
            }           

            public bool TryGetKey(string alias, out string key)
            {
                key = null;
                var hasKey = _inner.TryGetValue(alias, out var doc);

                if (hasKey)
                    key = doc.Id;

                return hasKey;
            }

            public void Set(StringSegment alias, Document val)
            {
                if (_inner == null)
                    _inner = new Dictionary<string, Document>();

                _inner.Add(alias, val);
            }            

            public void Populate(DynamicJsonValue j, string[] aliases, GraphQuery gq)
            {
                if (_inner == null)
                    return;

                var edgeAliases = aliases.Except(_inner.Keys).ToArray();
                foreach (var item in _inner)
                {
                    item.Value.EnsureMetadata();
                    j[item.Key] = item.Value.Data;

                    foreach (var alias in edgeAliases)
                    {
                        if (gq.WithEdgePredicates.TryGetValue(alias, out var edge) && 
                            edge.FromAlias.GetValueOrDefault() == item.Key &&
                            //TODO: Handle complex fields
                            item.Value.Data.TryGet(edge.Path.Compound[0], out object property))
                        {
                            j[alias] = property;
                        }
                    }
                }
            }

            public void PopulateVertices(DynamicJsonValue j)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    item.Value.EnsureMetadata();
                    j[item.Key] = item.Value.Data;                    
                }
            }

            public void PopulateVertices(IntermediateResults i)
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
                    item.Value.EnsureMetadata();

                    return item.Value;
                }
                throw new InvalidOperationException("Cannot return single result when there are no results");
            }

            internal Document GetResult(string alias)
            {
                var val = _inner[alias];
                val.EnsureMetadata();
                return val;
            }
        }
    }
}
