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
        public unsafe struct Match
        {
            private Dictionary<string, object> _inner;

            public int Count => _inner?.Count ?? 0;

            public IEnumerable<string> Aliases => _inner.Keys;

            public bool Empty => _inner == null || _inner.Count == 0;

            public override string ToString()
            {
                if (_inner == null)
                    return "<empty>";
                return string.Join(", ", _inner.Select(x=> x.Key + " - " + x.Value));
            }

            public Match(Match other)
            {
                if (other._inner == null)
                {
                    _inner = null;
                }
                else
                {
                    _inner = new Dictionary<string, object>(other._inner);
                }
            }

            public void Merge(Match other)
            {
                if(other._inner == null)
                    return;
                EnsureInnerInitialized();
                foreach (var item in other._inner)
                {
                    _inner[item.Key] = item.Value;
                }
            }

            public object GetResult(string alias)
            {
                object result = default;
                _inner?.TryGetValue(alias, out result);
                return result;
            }

            public Document GetSingleDocumentResult(string alias)
            {
                object result = default;
                _inner?.TryGetValue(alias, out result);
                if(result is Document d)
                {
                    d.EnsureMetadata();
                    return d;
                }
                else if(result is List<Match> m)
                {
                    // TODO: this is wrong
                    return m.Last().GetSingleDocumentResult(alias);
                }
                return null;
            }           

            public bool TryGetAliasId(string alias, out long id)
            {
                id = -1;

                if (_inner.TryGetValue(alias, out var result))
                {
                    if (result is Document d)
                    {
                        id = (long)d.Data.BasePointer;
                        return true;
                    }
                }

                return false;
            }

            //try to set, but don't overwrite
            public long? TrySet(StringSegment alias, Document val)
            {
                EnsureInnerInitialized();

                if (_inner.TryAdd(alias, val) == false)
                    return null;
                return (long)val.Data.BasePointer;
            }

            private void EnsureInnerInitialized()
            {
                if (_inner == null)
                    _inner = new Dictionary<string, object>();
            }

            public void Set(StringSegment alias, object val)
            {
                EnsureInnerInitialized();
                _inner[alias] = val;
            }

            public void PopulateVertices(DynamicJsonValue j)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    if (item.Key.StartsWith("_"))
                        continue;

                    if(item.Value is Document d)
                    {
                        j[item.Key] = d.Data;
                    }
                    else  if(item.Value is List<Match> matches)
                    {
                        var array = new DynamicJsonArray();
                        foreach (var m in matches)
                        {
                            var djv = new DynamicJsonValue();
                            m.PopulateVertices(djv);
                            array.Add(djv);
                        }
                        j[item.Key] = array;
                    }
                    else if(item.Value is string s)
                    {
                        j[item.Key] = s;
                    }
                }
            }

            internal Document GetFirstResult()
            {
                foreach (var item in _inner)
                {
                    if (item.Key.StartsWith("_"))
                        continue;

                    if (item.Value is Document d)
                    {
                        return d;
                    }
                }
                throw new InvalidOperationException("Cannot return single result when there are no results");
            }
        }
    }
}
