using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Server.Routing
{
    // How a request finds its route, with only these routes as an example:
    //
    //   GET  /databases/*/docs    GET  /databases/*/task    GET  /databases/*/tasks    GET  /studio/$
    //   POST /databases/*/docs    GET  /topology            GET  /info/tcp
    //
    // At startup we group the routes by method, and split the routes of each method:
    //
    //   GET   Routes      the whole path                    /topology, /info/tcp
    //         Databases   the path after the database name  /docs, /task, /tasks
    //         ByPrefix    the routes that end with '$'      /studio/
    //   POST  Databases                                     /docs
    //
    // Routes and Databases are a decision tree for each path length. An inner node selects by the char at a single position,
    // and a leaf holds the single route that remains:
    //
    //   GET Databases                               GET Routes
    //     length 5:  path[1]  'd' -> "/docs"          length 9:  path[1]  'i' -> "/info/tcp"
    //                         't' -> "/task"                              't' -> "/topology"
    //     length 6:  "/tasks" (a single route, so it is a leaf)
    //
    // GET /databases/Northwind/docs
    //   1. the method is GET (see FindMethod)
    //   2. it starts with /databases/, so the name is "Northwind" (until the next '/') and the rest is "/docs"
    //   3. GET Databases, length 5: path[1] is 'd' -> the leaf "/docs", the whole rest is "/docs" -> match
    //
    // GET /topology               not /databases/, GET Routes, length 9: path[1] is 't' -> "/topology" -> match
    // GET /databases/db/dogs      GET Databases, length 5: path[1] is 'd' -> "/docs", but "/dogs" is not "/docs" -> no match
    // POST /databases/db/tasks    POST Databases has no route of length 6 -> no match
    // GET /studio/index.html      GET Routes has no route of length 18, the path starts with /studio/ -> GET /studio/$
    //
    // A node checks a single char, so the compare at the leaf decides. A node folds the char to lower case (see Fold), and the
    // leaf compare ignores case, so the match ignores case. The nodes of all the trees are in _nodes, and their child slots
    // are in _children.
    public sealed class RouteMatcher<T>
    {
        private readonly struct Node(int position, char low, char high, int childrenStart, int value, string literal)
        {
            // the position of the char that an inner node selects by, -1 for a leaf
            public readonly int Position = position;

            // an inner node has a slot in _children for every folded char from Low to High, the index of the next node (0 if there is none)
            public readonly char Low = low;
            public readonly char High = high;
            public readonly int ChildrenStart = childrenStart;

            // the index in _values of the route of a leaf, -1 for node 0, which matches nothing
            public readonly int Value = value;
            public readonly string Literal = literal;

            public bool IsLeaf => Position < 0;

            // PERF: clients send the path as the route was written, so we check the exact match first
            public bool IsMatch(ReadOnlySpan<char> path) => Value >= 0 && (path.SequenceEqual(Literal) || Ascii.EqualsIgnoreCase(path, Literal));

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int FindNext(ReadOnlySpan<char> path, int[] children)
            {
                int offset = Fold(path[Position]) - Low;
                if ((uint)offset > (uint)(High - Low))
                    return 0;

                return children[ChildrenStart + offset];
            }
        }

        private sealed class MethodRoutes(string method, int[] routes, int[] databases, int[] byPrefix)
        {
            public readonly string Method = method;

            // the root node of the routes of each length, 0 is the no match node
            public readonly int[] Routes = routes;

            // the same, for the path after the database name of the routes that start with /databases/* (null if there are none)
            public readonly int[] Databases = databases;

            // the value of the route that ends with '$' for each of the Prefixes, -1 if there is none (empty if there are none at all)
            public readonly int[] ByPrefix = byPrefix;
        }

        // PERF: most requests are for a database, comparing to a constant is a couple of vector instructions, as the JIT unrolls it
        private const string DatabasesPrefix = "/databases/";
        private const string DatabasesCapture = DatabasesPrefix + "*";

        // PERF: the prefixes of the routes that end with '$', we compare them as constants in FindPrefix
        private const string StudioPrefix = "/studio/";
        private const string WizardPrefix = "/wizard/";
        private const string EulaPrefix = "/eula/";
        private const string TwoFactorPrefix = "/2fa/";
        private static readonly string[] Prefixes = [StudioPrefix, WizardPrefix, EulaPrefix, TwoFactorPrefix];

        private readonly MethodRoutes[] _methods;
        private readonly T[] _values;
        private readonly Node[] _nodes;
        private readonly int[] _children;

        public RouteMatcher(IEnumerable<(string Method, string Path, T Value)> routes)
        {
            var builder = new Builder();
            var values = new List<T>();
            var byMethod = new Dictionary<string, (List<(string, int)> Routes, List<(string, int)> Databases, int[] ByPrefix)>(StringComparer.OrdinalIgnoreCase);

            foreach (var (method, path, value) in routes)
            {
                EnsureIsAscii(method, path);

                if (byMethod.TryGetValue(method, out var methodRoutes) == false)
                    byMethod[method] = methodRoutes = (new List<(string, int)>(), new List<(string, int)>(), Enumerable.Repeat(-1, Prefixes.Length).ToArray());

                values.Add(value);
                int id = values.Count - 1;

                if (path.Contains('$'))
                {
                    int prefix = Array.FindIndex(Prefixes, x => string.Equals(path, x + "$", StringComparison.OrdinalIgnoreCase));
                    if (prefix < 0)
                        throw new InvalidOperationException($"A route can end with '$' only after one of {string.Join(", ", Prefixes)}, but got: {method} {path}");

                    if (methodRoutes.ByPrefix[prefix] >= 0)
                        throw new InvalidOperationException($"A duplicate route found: {method} {path}");

                    methodRoutes.ByPrefix[prefix] = id;
                    continue;
                }

                int capture = path.IndexOf('*');
                if (capture < 0)
                {
                    if (path.StartsWith(DatabasesPrefix, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"A route that starts with '{DatabasesPrefix}' must capture the database name, as '{DatabasesCapture}/', but got: {method} {path}");

                    methodRoutes.Routes.Add((path, id));
                    continue;
                }

                if (path.StartsWith(DatabasesCapture, StringComparison.OrdinalIgnoreCase) == false || capture != DatabasesPrefix.Length ||
                    path.IndexOf('*', capture + 1) >= 0 || (capture + 1 < path.Length && path[capture + 1] != '/'))
                    throw new InvalidOperationException($"A route can capture only the database name, as '{DatabasesCapture}/', but got: {method} {path}");

                methodRoutes.Databases.Add((path[(capture + 1)..], id));
            }

            _methods = byMethod
                .OrderByDescending(x => x.Value.Routes.Count + x.Value.Databases.Count)
                .Select(x => new MethodRoutes(
                    // PERF: Kestrel gives the known methods as the interned literals, so the common case is a reference comparison
                    string.Intern(x.Key.ToUpperInvariant()),
                    builder.Build(x.Key, x.Value.Routes),
                    x.Value.Databases.Count == 0 ? null : builder.Build(x.Key, x.Value.Databases),
                    x.Value.ByPrefix.Any(id => id >= 0) ? x.Value.ByPrefix : []))
                .ToArray();

            _values = values.ToArray();
            _nodes = builder.Nodes.ToArray();
            _children = builder.Children.ToArray();
        }

        public bool TryMatch(string method, string path, out RouteMatch match, out T value)
        {
            path ??= string.Empty;
            match = new RouteMatch
            {
                Url = path,
                Method = method,
                MatchLength = path.Length
            };

            var methodRoutes = FindMethod(method);
            if (methodRoutes != null)
            {
                ReadOnlySpan<char> span = path;

                int id;
                if (methodRoutes.Databases != null &&
                    span.StartsWith(DatabasesPrefix, StringComparison.OrdinalIgnoreCase) &&
                    (id = FindDatabaseRoute(methodRoutes.Databases, span, ref match)) >= 0)
                {
                    value = _values[id];
                    return true;
                }

                id = Find(methodRoutes.Routes, span);
                if (id >= 0)
                {
                    value = _values[id];
                    return true;
                }

                if (methodRoutes.ByPrefix.Length != 0)
                {
                    int prefix = FindPrefix(span);
                    if (prefix >= 0 && (id = methodRoutes.ByPrefix[prefix]) >= 0)
                    {
                        match.MatchLength = Prefixes[prefix].Length;
                        value = _values[id];
                        return true;
                    }
                }
            }

            value = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FindDatabaseRoute(int[] routes, ReadOnlySpan<char> path, ref RouteMatch match)
        {
            var name = path.Slice(DatabasesPrefix.Length);
            int nameLength = name.IndexOf('/');
            if (nameLength < 0)
                nameLength = name.Length;

            int id = Find(routes, name.Slice(nameLength));
            if (id >= 0)
            {
                match.CaptureStart = DatabasesPrefix.Length;
                match.CaptureLength = nameLength;
            }

            return id;
        }

        // PERF: _methods is sorted by the number of routes (GET, POST, ...), so we usually find the method in 1 to 3
        // reference compares, which is cheaper than hashing the method
        private MethodRoutes FindMethod(string method)
        {
            var methods = _methods;
            foreach (var routes in methods)
            {
                if (ReferenceEquals(routes.Method, method))
                    return routes;
            }

            foreach (var routes in methods)
            {
                if (Ascii.EqualsIgnoreCase(routes.Method, method))
                    return routes;
            }

            return null;
        }

        // returns the index of the value of the route, -1 if there is none
        private int Find(int[] rootByLength, ReadOnlySpan<char> path)
        {
            if ((uint)path.Length >= (uint)rootByLength.Length)
                return -1;

            var nodes = _nodes;
            var children = _children;
            int index = rootByLength[path.Length];
            while (true)
            {
                ref readonly var node = ref nodes[index];
                if (node.IsLeaf)
                    return node.IsMatch(path) ? node.Value : -1;

                index = node.FindNext(path, children);
            }
        }

        // returns the index in Prefixes of the prefix that the path starts with, -1 if there is none
        private static int FindPrefix(ReadOnlySpan<char> path)
        {
            if (path.StartsWith(StudioPrefix, StringComparison.OrdinalIgnoreCase))
                return 0;
            if (path.StartsWith(WizardPrefix, StringComparison.OrdinalIgnoreCase))
                return 1;
            if (path.StartsWith(EulaPrefix, StringComparison.OrdinalIgnoreCase))
                return 2;
            if (path.StartsWith(TwoFactorPrefix, StringComparison.OrdinalIgnoreCase))
                return 3;
            return -1;
        }

        private static void EnsureIsAscii(string method, string path)
        {
            if (Ascii.IsValid(method) == false || Ascii.IsValid(path) == false)
                throw new InvalidOperationException($"Cannot use non ASCII chars in routes, but got: {method} {path}");
        }

        // PERF: folds the ASCII letters to lower case, so a node needs child slots for a single case. It also folds a few other
        // chars together (e.g. '_' and DEL), the leaf compare rejects those.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Fold(char c) => c | 0x20;

        private sealed class Builder
        {
            // node 0 is the no match node
            public readonly List<Node> Nodes = [new Node(position: -1, low: default, high: default, childrenStart: 0, value: -1, literal: null)];
            public readonly List<int> Children = [];

            // returns the root node of the routes of each length
            public int[] Build(string method, List<(string Path, int Value)> routes)
            {
                var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var (path, _) in routes)
                {
                    if (unique.Add(path) == false)
                        throw new InvalidOperationException($"A duplicate route found: {method} {path}");
                }

                if (routes.Count == 0)
                    return [];

                var rootByLength = new int[routes.Max(x => x.Path.Length) + 1];
                foreach (var sameLength in routes.GroupBy(x => x.Path.Length))
                    rootByLength[sameLength.Key] = AddNode(sameLength.ToList());

                return rootByLength;
            }

            private int AddNode(List<(string Path, int Value)> candidates)
            {
                if (candidates.Count == 1)
                {
                    Nodes.Add(new Node(position: -1, low: default, high: default, childrenStart: 0, value: candidates[0].Value, literal: candidates[0].Path));
                    return Nodes.Count - 1;
                }

                // the position whose largest group is the smallest, and on a tie the one with more groups
                int position = -1;
                int largest = int.MaxValue;
                List<IGrouping<int, (string Path, int Value)>> groups = null;
                for (int i = 0; i < candidates[0].Path.Length; i++)
                {
                    var byChar = candidates.GroupBy(x => Fold(x.Path[i])).ToList();
                    int byCharLargest = byChar.Max(g => g.Count());
                    if (byCharLargest < largest || (byCharLargest == largest && byChar.Count > groups.Count))
                    {
                        position = i;
                        largest = byCharLargest;
                        groups = byChar;
                    }
                }

                if (largest == candidates.Count)
                    throw new InvalidOperationException($"Cannot tell apart the routes: {string.Join(", ", candidates.Select(x => x.Path))}");

                var children = groups.Select(g => (Folded: g.Key, Node: AddNode(g.ToList()))).ToList();

                char low = (char)children.Min(x => x.Folded);
                char high = (char)children.Max(x => x.Folded);
                int childrenStart = Children.Count;
                for (int c = low; c <= high; c++)
                    Children.Add(0);
                foreach (var (c, node) in children)
                    Children[childrenStart + c - low] = node;

                Nodes.Add(new Node(position, low, high, childrenStart, value: -1, literal: null));
                return Nodes.Count - 1;
            }
        }
    }
}
