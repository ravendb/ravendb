// -----------------------------------------------------------------------
//  <copyright file="Trie.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Raven.Server.Routing
{
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
        public Trie<T>[] FilteredChildren =>  Children?.Where(x => x != null).Distinct().ToArray();
         
        public struct Match
        {
            public T Value;
            public string Url;
            public bool Success;
            public int CaptureStart;
            public int CaptureLength;
            public int MatchLength;
        }

        public Match TryMatch(string url)
        {
            var match = new Match
            {
                Url = url
            };
            var current = this;
            var currentIndex = 0;
            for (int i = 0; i < url.Length; i++)
            {
                if (currentIndex < current.Key.Length)
                {
                    // if(current.Key[currentIndex] != url[i])
                    if (CharEqualsAt(current.Key, currentIndex, url, i) == false)
                    {
                        if (current.Key[currentIndex] == '$')
                        {
                            match.Success = true;
                            match.MatchLength = i;
                            match.Value = current.Value;
                            return match;
                        }
                        return match;
                    }
                    currentIndex++;
                    continue;
                }
                // end of node, need to search children
                var maybe = (url[i] <= current.Children.Length)
                    ? current.Children[url[i]]
                    : null;

                if (maybe == null)
                {
                    maybe = current.Children['*'];
                    if (maybe != null)
                    {
                        match.CaptureStart = i;
                        for (; i < url.Length; i++)
                        {
                            if (url[i] == '/')
                            {
                                break;
                            }
                        }
                        match.CaptureLength = i - match.CaptureStart;
                        i--;
                    }
                }
                current = maybe;
                currentIndex = 1;
                if (current == null)
                {
                    return match;
                }
            }
            match.Value = current.Value;
            match.MatchLength = url.Length;
            match.Success = true;
            return match;
        }

        public override string ToString()
        {
            return $"Key: {Key}, Children: {Children?.Distinct().Count(x=>x!=null)}";
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
                current.Value = source[sortedKeys[start]];
                return;
            }
            current.Children = new Trie<T>[127];
            var minKey = sortedKeys[start];
            var maxKey = sortedKeys[start + count - 1];
            var matchingIndex = matchStart == 0 ? matchStart : matchStart + 1;
            if (matchStart <= 0 || minKey[matchStart] != '*')
            {
                for (int i = matchingIndex; i < Math.Min(minKey.Length, maxKey.Length); i++)
                {
                    if (minKey[i] == maxKey[i] &&
                        minKey[i] != '*')
                        continue;
                    matchingIndex = i;
                    break;
                }
            }


            if (maxKey.StartsWith(minKey))
            {
                current.Value = source[minKey];
                current.Key = minKey.Substring(matchStart, minKey.Length - matchStart);
                AddChild(current, source, sortedKeys, minKey.Length, start + 1, count - 1);
                return;
            }

            current.Key = minKey.Substring(matchStart, matchingIndex - matchStart);
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

        private static void AddChild(Trie<T> current, Dictionary<string, T> source, string[] sortedKeys, int matchingIndex, int childStart,
            int childCount)
        {
            Trie<T> child;
            child = new Trie<T>();
            Build(child, source, sortedKeys, matchingIndex, childStart, childCount);
            current.Children[char.ToUpper(child.Key[0])] = child;
            current.Children[char.ToLower(child.Key[0])] = child;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CharEqualsAt(string x, int xIndex,string y, int yIndex)
        {
            if (x[xIndex] == y[yIndex])
                return true;
            if (x[xIndex] > 'Z')
                return (x[xIndex] - 'A' + 'a') == y[yIndex];
            return x[xIndex] == (y[yIndex] - 'A' + 'a');
        }
    }
}