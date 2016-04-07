// -----------------------------------------------------------------------
//  <copyright file="Trie.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Routing
{
    public struct RouteMatch
    {
        public string Url;
        public string Method;
        public int CaptureStart;
        public int CaptureLength;
        public int MatchLength;

        public StringSegment GetCapture()
        {
            return new StringSegment(Url, CaptureStart, CaptureLength);
        }
    }

    /// <summary>
    /// We use this trie for speedy routing, by matching parts of the 
    /// urls in the trie. With the notion that * in route URL will 
    /// match anything until the next /.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Trie<T>
    {
        public string Key;
        public T Value;
        public Trie<T>[] Children;
        public Trie<T>[] FilteredChildren => Children?.Where(x => x != null).Distinct().ToArray();

        public struct MatchResult
        {
            public RouteMatch Match;
            public T Value;
            public int CurrentIndex;

            public Trie<T> SearchTrie(Trie<T> current, string term)
            {
                for (int i = 0; i < term.Length; i++)
                {
                    if (CurrentIndex < current.Key.Length)
                    {
                        // if(current.Key[currentIndex] != url[i])
                        if (CharEqualsAt(current.Key, CurrentIndex, term, i) == false)
                        {
                            if (current.Key[CurrentIndex] == '$')
                            {
                                Match.MatchLength = i;
                                Value = current.Value;
                                return current;
                            }
                            return current;
                        }
                        CurrentIndex++;
                        continue;
                    }
                    // end of node, need to search children
                    var maybe = term[i] <=  current.Children.Length
                        ? current.Children[term[i]]
                        : null;

                    if (maybe == null)
                    {
                        maybe = current.Children['*'];
                        if (maybe != null)
                        {
                            Match.CaptureStart = i;
                            for (; i < term.Length; i++)
                            {
                                if (term[i] == '/')
                                {
                                    break;
                                }
                            }
                            Match.CaptureLength = i - Match.CaptureStart;
                            i--;
                        }
                        else
                        {
                            maybe = current.Children['$'];
                            if (maybe != null)
                            {
                                CurrentIndex = 0;
                                Match.MatchLength = i;
                                Value = maybe.Value;
                                return maybe;
                            }
                        }
                    }
                    current = maybe;
                    CurrentIndex = 1;
                    if (current == null)
                    {
                        return null;
                    }
                }
                Match.MatchLength = term.Length;
                return current;
            }

        }

        public MatchResult TryMatch(string method, string url)
        {
            var match = new MatchResult
            {
                Match =
                {
                    Url = url,
                    Method = method
                }
            };

            var result = match.SearchTrie(this, method);
            if (result == null)
            {
                return match;
            }

            result = match.SearchTrie(result, url);
            if (result == null || 
                 (match.CurrentIndex != result.Key.Length && result.Key[match.CurrentIndex] != '$')
               )
            {
                return match;
            }

            match.Value = result.Value;
            return match;
        }

        public override string ToString()
        {
            return $"Key: {Key}, Children: {Children?.Distinct().Count(x => x != null)}";
        }

        public static Trie<T> Build(Dictionary<string, T> source)
        {
            var sortedKeys = source.Keys.ToArray();
            Array.Sort(sortedKeys, StringComparer.OrdinalIgnoreCase);
            // to simplify things, we require that the routs be in ASCII only
            EnsureRoutsAreOnlyUsingASCII(sortedKeys);

            var trie = new Trie<T>();

            Build(trie, source, sortedKeys, 0, 0, sortedKeys.Length);

            return trie;
        }

        private static void EnsureRoutsAreOnlyUsingASCII(string[] sortedKeys)
        {
            foreach (var sortedKey in sortedKeys)
            {
                for (int i = 0; i < sortedKey.Length; i++)
                {
                    if (sortedKey[i] >= 127)
                        throw new InvalidOperationException("Cannot use non ASCII chars in routes, but got: " + sortedKey);
                }
            }
        }

        private static void Build(Trie<T> current, Dictionary<string, T> source, string[] sortedKeys, int matchStart, int start, int count)
        {
            if (count == 1)
            {
                // just one entry, build the trie node
                current.Key = sortedKeys[start].Substring(matchStart, sortedKeys[start].Length - matchStart);
                if (HandleStarRoute(current, source, sortedKeys, matchStart, start, count))
                    return;

                current.Value = source[sortedKeys[start]];
                return;
            }
            current.Children = new Trie<T>[127];
            var minKey = sortedKeys[start];
            var maxKey = sortedKeys[start + count - 1];
            var matchingIndex = matchStart;
            for (int i = matchingIndex; i < Math.Min(minKey.Length, maxKey.Length); i++)
            {
                if (minKey[i] == maxKey[i])
                    continue;
                matchingIndex = i;
                break;
            }


            if (maxKey.StartsWith(minKey))
            {
                current.Value = source[minKey];
                current.Key = minKey.Substring(matchStart, minKey.Length - matchStart);
                AddChild(current, source, sortedKeys, minKey.Length, start + 1, count - 1);
                return;
            }

            current.Key = minKey.Substring(matchStart, matchingIndex - matchStart);
            if (HandleStarRoute(current, source, sortedKeys, matchStart, start, count))
                return;
            var childStart = start;
            var childCount = 1;

            while (childStart + childCount < start + count)
            {
                var nextKey = sortedKeys[childStart + childCount];
                if (matchingIndex < nextKey.Length && CharEqualsAt(nextKey, matchingIndex, minKey, matchingIndex))
                {
                    childCount++;
                    continue;
                }
                minKey = nextKey;
                AddChild(current, source, sortedKeys, matchingIndex, childStart, childCount);
                childStart += childCount;
                childCount = 1;
            }
            AddChild(current, source, sortedKeys, matchingIndex, childStart, childCount);
        }

        private static bool HandleStarRoute(Trie<T> current, Dictionary<string, T> source, string[] sortedKeys, int matchStart, int start,
            int count)
        {
            var indexOfStar = current.Key.IndexOf('*');
            if (indexOfStar == -1)
                return false;
            var tmp = current.Key;
            current.Key = tmp.Substring(0, indexOfStar);
            if (current.Children == null)
                current.Children = new Trie<T>[127];
            current.Children['*'] = new Trie<T>
            {
                Key = "*",
                Children = new Trie<T>[127]
            };
            AddChild(current.Children['*'], source, sortedKeys, matchStart + indexOfStar + 1, start, count);
            return true;
        }

        private static void AddChild(Trie<T> current, Dictionary<string, T> source, string[] sortedKeys, int matchingIndex, int childStart,
            int childCount)
        {
            var child = new Trie<T>();
            Build(child, source, sortedKeys, matchingIndex, childStart, childCount);
            if (child.Key.Length == 0)
            {
                current.Children = child.Children;
            }
            else
            {
                current.Children[char.ToUpper(child.Key[0])] = child;
                current.Children[char.ToLower(child.Key[0])] = child;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CharEqualsAt(string x, int xIndex, string y, int yIndex)
        {
            if (x[xIndex] == y[yIndex])
                return true;			

            if (x[xIndex] > 'Z' && y[yIndex] <= 'Z')
                return y[yIndex] - 'A' + 'a' == x[xIndex];
            if (x[xIndex] <= 'Z' && y[yIndex] > 'Z')
                return x[xIndex] - 'A' + 'a' == y[yIndex];

            return y[yIndex] - 'A' + 'a' == x[xIndex] - 'A' + 'a';

        }
    }
}