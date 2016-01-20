// -----------------------------------------------------------------------
//  <copyright file="Trie.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

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

        public bool TryGetValue(string route, out T value)
        {
            var current = this;
            var currentIndex = 0;
            for (int i = 0; i < route.Length; i++)
            {
                if (currentIndex < current.Key.Length)
                {
                    if (current.Key[currentIndex] == route[i])
                    {
                        currentIndex++;
                        continue;
                    }
                    value = default(T);
                    return false;
                }
                // end of node, need to search children
                if (current.Children.Length == 0)
                {
                    value = default(T);
                    return false;
                }
                //TODO: Optimize this to binary search?
                for (int j = 0; j < current.Children.Length; j++)
                {
                    if (current.Children[j].Key[0] == route[i])
                    {
                        current = current.Children[j];
                        currentIndex = 1;
                        break;
                    }
                    if (j == current.Children.Length - 1)
                    {
                        value = default(T);
                        return false;
                    }
                }
            }
            value = current.Value;
            return true;

        }

        public override string ToString()
        {
            return $"Key: {Key}, Children: {Children?.Length ?? 0}";
        }

        public static Trie<T> Build(Dictionary<string, T> source)
        {
            var sortedKeys = source.Keys.ToArray();
            Array.Sort(sortedKeys, StringComparer.OrdinalIgnoreCase);

            var trie = new Trie<T>();

            Build(trie, source, sortedKeys, 0, 0, sortedKeys.Length);

            return trie;
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
            var minKey = sortedKeys[start];
            var maxKey = sortedKeys[start + count - 1];
            var matchingIndex = matchStart == 0 ? matchStart : matchStart + 1;
            if (matchStart > 0 && minKey[matchStart] == '*')
            {
            }
            else
            {
                for (int i = matchingIndex; i < Math.Min(minKey.Length, maxKey.Length); i++)
                {
                    if (minKey[i] == maxKey[i] && minKey[i] != '*')
                        continue;
                    matchingIndex = i;
                    break;
                }
            }

            current.Key = minKey.Substring(matchStart, matchingIndex - matchStart);
            var children = new List<Trie<T>>();

            var childStart = start;
            var childCount = 1;
            Trie<T> child;
            while (childStart + childCount < start + count)
            {
                var nextKey = sortedKeys[childStart + childCount];
                if (matchingIndex < nextKey.Length && CharEqualsAt(nextKey, minKey, matchingIndex))
                {
                    childCount++;
                    continue;
                }
                minKey = nextKey;
                child = new Trie<T>();
                Build(child, source, sortedKeys, matchingIndex, childStart, childCount);
                children.Add(child);
                childStart += childCount;
                childCount = 1;
            }
            child = new Trie<T>();
            Build(child, source, sortedKeys, matchingIndex, childStart, childCount);
            children.Add(child);

            current.Children = children.ToArray();
        }

        private static bool CharEqualsAt(string x, string y, int matchingIndex)
        {
            return string.Compare(x, matchingIndex, y, matchingIndex, 1, StringComparison.OrdinalIgnoreCase) == 0;
        }
    }
}