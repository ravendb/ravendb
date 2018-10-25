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
            public struct Result
            {
                public Document Single;
                public List<Document> Multiple;
            }
            private Dictionary<string, Result> _inner;

            public object Key => _inner;

            public int Count => _inner?.Count ?? 0;

            public IEnumerable<string> Aliases => _inner.Keys;

            public bool Empty => _inner == null || _inner.Count == 0;

            public override string ToString()
            {
                if (_inner == null)
                    return "<empty>";
                return string.Join(", ", _inner.Select(x=> x.Key + " - " + (x.Value.Single?.Id ?? x.Value.Single?.Data?.ToString()) ?? "multi"));
            }

            public Match(Match other)
            {
                if (other._inner == null)
                {
                    _inner = null;
                }
                else
                {
                    _inner = new Dictionary<string, Result>(other._inner);
                }
            }

            public Result GetResult(string alias)
            {
                Result result = default;
                _inner?.TryGetValue(alias, out result);
                return result;
            }

            public Document GetSingleDocumentResult(string alias)
            {
                Result result = default;
                _inner?.TryGetValue(alias, out result);
                if(result.Single?.Id != null)
                {
                    result.Single.EnsureMetadata();
                }
                return result.Single;
            }           

            public bool TryGetAliasId(string alias, out long id)
            {
                id = -1;

                if (_inner.TryGetValue(alias, out var result))
                {
                    if (result.Single != null)
                    {
                        id = (long)result.Single.Data.BasePointer;
                        return true;
                    }
                }

                return false;
            }

            //try to set, but don't overwrite
            public long? TrySet(StringSegment alias, Document val)
            {
                EnsureInnerInitialized();

                if (_inner.TryAdd(alias, new Result { Single = val }) == false)
                    return null;
                return (long)val.Data.BasePointer;
            }

            private void EnsureInnerInitialized()
            {
                if (_inner == null)
                    _inner = new Dictionary<string, Result>();
            }

            public void Set(StringSegment alias, Document val)
            {
                EnsureInnerInitialized();

                _inner.Add(alias, new Result { Single = val });
            }

            public void Set(StringSegment alias, Result r)
            {
                EnsureInnerInitialized();

                _inner.Add(alias, r);
            }

            public void ForceSet(StringSegment alias, List<Document> vals)
            {
                EnsureInnerInitialized();

                _inner[alias] = new Result { Multiple = vals };
            }


            public void PopulateVertices(DynamicJsonValue j)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    if(item.Value.Single != null)
                    {
                        var doc = item.Value.Single;
                        if (doc.Id != null)
                        {
                            doc.EnsureMetadata();
                        }
                        j[item.Key] = doc.Data;
                    }
                    else 
                    {
                        j[item.Key] = item.Value.Multiple;
                    }
                }
            }

            public void PopulateVertices(ref IntermediateResults i)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    if(item.Value.Single != null)
                    {
                        i.Add(item.Key, this, item.Value.Single);
                    }
                }
            }

            internal Document GetFirstResult()
            {
                foreach (var item in _inner)
                {
                    var doc = item.Value.Single;
                    if (doc == null)
                        continue;
                    if (doc.Id != null)
                    {
                        doc.EnsureMetadata();
                    }

                    return doc;
                }
                throw new InvalidOperationException("Cannot return single result when there are no results");
            }
        }
    }
}
