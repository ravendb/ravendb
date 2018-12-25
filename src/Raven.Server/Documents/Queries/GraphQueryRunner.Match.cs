using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Primitives;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner
    {
        public struct PathSegment : IEquatable<PathSegment>
        {
            public readonly long From;
            public readonly long To;

            public PathSegment(long @from, long to)
            {
                From = @from;
                To = to;
            }

            public bool Equals(PathSegment other)
            {
                return From == other.From && To == other.To;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is PathSegment segment && Equals(segment);
            }

            public override int GetHashCode() => HashCode.Combine(From, To);
        }

        public class MatchCollection : IEquatable<MatchCollection>, IEnumerable<Match>
        {
            private List<Match> _data = new List<Match>();

            public IEnumerator<Match> GetEnumerator() => _data.GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
            public void Add(Match item) => _data.Add(item);

            public void AddRange(IEnumerable<Match> items)
            {
                foreach (var item in items)
                    _data.Add(item);
            }

            public void Reverse() => _data.Reverse();
            public void Clear() => _data.Clear();
            public int Count => _data.Count;
            public bool IsReadOnly => false;

            public bool Equals(MatchCollection other)
            {
                if (ReferenceEquals(null, other)) 
                    return false;
                return ReferenceEquals(this, other) || Equals(other._data);
            }

            public bool Equals(List<Match> other)
            {
                if (other?.Count != Count)
                    return false;
                for (var i = 0; i < _data.Count; i++)
                {
                    if (_data[i].Equals(other[i]) == false)
                        return false;
                }
                return true;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) 
                    return false;
                if (ReferenceEquals(this, obj)) 
                    return true;

                return obj.GetType() == GetType() && Equals((MatchCollection)obj);
            }

            public override int GetHashCode()
            {
                int hash = Count;

                foreach (var item in _data)
                {
                    hash = HashCode.Combine(item.GetHashCode(), hash);
                }
                return hash;
            }
        }

        public unsafe struct Match : IEquatable<Match>
        {
            private Dictionary<string, object> _inner;

            public int Count => _inner?.Count ?? 0;

            public IEnumerable<string> Aliases => _inner?.Keys ?? Enumerable.Empty<string>();

            public bool Empty => _inner == null || _inner.Count == 0;

            public bool Equals(Match other)
            {
                if (other.Count != Count)
                    return false;

                foreach (var kvp in _inner)
                {
                    if (other._inner.TryGetValue(kvp.Key, out var otherVal) == false)
                        return false;

                    if (Equals(kvp.Value, otherVal) == false)
                        return false;
                }

                return true;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is Match match && Equals(match);
            }

            public override string ToString()
            {
                if (_inner == null)
                    return "<empty>";
                return string.Join(", ", _inner.Select(x => x.Key + " - " + GetNameOfValue(x.Value)));
            }

            public override int GetHashCode()
            {
                int hash = Count;

                foreach (var item in _inner)
                {
                    hash = HashCode.Combine(hash, item.Key.GetHashCode(), item.Value?.GetHashCode() ?? -1);
                }

                return hash;
            }

            private static object GetNameOfValue(object x)
            {
                if (x is Document d && d.Id != null)
                    return d.Id;

                return x;
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

            public void Remove(string alias)
            {
                if (_inner == null)
                    return;
                _inner.Remove(alias);
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

            private void EnsureInnerInitialized()
            {
                if (_inner == null)
                    _inner = new Dictionary<string, object>();
            }

            public void Set(StringSegment alias, object val)
            {
                EnsureInnerInitialized();
                _inner[alias.Value] = val;
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
                    else  if(item.Value is MatchCollection matches)
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
                    else if(item.Value is BlittableJsonReaderBase b)
                    {
                        j[item.Key] = b;
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
