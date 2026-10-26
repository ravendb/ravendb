using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Indexes
{
    public class DynamicEnumerableIndexesTests :RavenTestBase
    {
        public DynamicEnumerableIndexesTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("short", "none", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("short", "null", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("short", "default", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "none", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "null", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "default", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("short", "none", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("short", "null", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("short", "default", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "none", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "null", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "default", true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesTypedDerivedEnumeratorAndDisposalDuringIndexing(Options options, string type, string comparer, bool objectBound)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/TypedDerivedContains";
            string elementType = objectBound ? "object" : type;
            string needle = (objectBound ? "(object)" : "") + "(" + type + ")item.Target";
            string comparerArgument = comparer switch
            {
                "null" => ", (IEqualityComparer<" + elementType + ">)null",
                "default" => ", EqualityComparer<" + elementType + ">.Default",
                _ => ""
            };
            string expression = "Enumerable.Contains((IEnumerable<" + elementType + ">)values, " + needle + comparerArgument + ")";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ContainsOriginItems let values = new TypedContainsValues(item.Values, " + (type == "char" ? "true" : "false") + ") select new { item.Key, Found = " + expression + ", Trace = values.Trace.ToArray() }" },
                AdditionalSources = { ["TypedContainsValues.cs"] = """
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Indexes.Static;
public sealed class TypedContainsValues : DynamicArray, IEnumerable<short>, IEnumerable<char>, IEnumerable<object>
{
    private readonly object[] _values;
    public readonly List<string> Trace = new List<string>();
    public TypedContainsValues(IEnumerable values, bool character)
        : this(values.Cast<object>().Select(value => character ? (object)(char)Convert.ToInt32(value) : (object)(short)Convert.ToInt32(value)).ToArray()) { }
    private TypedContainsValues(object[] values) : base(values) { _values = values; }
    IEnumerator<short> IEnumerable<short>.GetEnumerator()
    {
        Trace.Add("short.GetEnumerator");
        return EnumerateShorts().GetEnumerator();
    }
    private IEnumerable<short> EnumerateShorts()
    {
        try
        {
            foreach (object value in _values)
            {
                Trace.Add("short.MoveNext:" + Convert.ToInt32(value));
                yield return (short)value;
            }
        }
        finally { Trace.Add("short.Dispose"); }
    }
    IEnumerator<char> IEnumerable<char>.GetEnumerator()
    {
        Trace.Add("char.GetEnumerator");
        return EnumerateCharacters().GetEnumerator();
    }
    private IEnumerable<char> EnumerateCharacters()
    {
        try
        {
            foreach (object value in _values)
            {
                Trace.Add("char.MoveNext:" + Convert.ToInt32(value));
                yield return (char)value;
            }
        }
        finally { Trace.Add("char.Dispose"); }
    }
    IEnumerator<object> IEnumerable<object>.GetEnumerator()
    {
        Trace.Add("object.GetEnumerator");
        return EnumerateObjects().GetEnumerator();
    }
    private IEnumerable<object> EnumerateObjects()
    {
        try
        {
            foreach (object value in _values)
            {
                Trace.Add("object.MoveNext:" + Convert.ToInt32(value));
                yield return value;
            }
        }
        finally { Trace.Add("object.Dispose"); }
    }
    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<object>)this).GetEnumerator();
}
""" },
                Fields =
                {
                    ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["Trace"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
                }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new ContainsOriginItem { Key = "hit", Values = type == "char" ? new[] { 97L, 98L } : new[] { 5L, 6L }, Target = type == "char" ? 97 : 5 });
                session.Store(new ContainsOriginItem { Key = "miss", Values = type == "char" ? new[] { 99L, 100L } : new[] { 4L, 6L }, Target = type == "char" ? 97 : 5 });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                ContainsOriginResult[] rows = session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found, Trace").ToList().OrderBy(row => row.Key).ToArray();
                Assert.Equal(new[] { ("hit", true), ("miss", false) }, rows.Select(row => (row.Key, row.Found)).ToArray());
                string route = objectBound ? "object" : type;
                string hit = route + ".GetEnumerator|" + route + ".MoveNext:" + (type == "char" ? "97" : "5") + "|" + route + ".Dispose";
                string miss = route + ".GetEnumerator|" + route + ".MoveNext:" + (type == "char" ? "99" : "4") + "|" + route + ".MoveNext:" + (type == "char" ? "100" : "6") + "|" + route + ".Dispose";
                Assert.Equal(new[] { ("hit", hit), ("miss", miss) }, rows.Select(row => (row.Key, string.Join("|", row.Trace))).ToArray());
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("new string[] { \"b\", \"a\" }.OrderBy(x => x.Length > 100 ? x.Missing : x).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new string[] { \"b\", \"a\" }.OrderBy(x => null).First() == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new string[] { \"b\", \"a\" }.OrderBy(x => x.Length > 100 ? x.Missing.Value : x).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new[] { \"b\", \"a\" }.OrderBy(x => x.Length > 100 ? x.Missing : x.ToUpper()).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new[] { \"b\", \"a\" }.OrderBy((x) => x).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrStringOrderingPreservesLegacyLambdaBindingDuringIndexing(Options options, string expression)
        {
            AssertPersistedStringOrderingExpression(options, expression);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("new string[] { \"b\", \"a\" }.OrderBy(string (x) => null).First() == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new string[] { \"b\", \"a\" }.OrderBy((x) => null).First() == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new string[] { \"b\", \"a\" }.OrderBy(x => null, (IComparer<string>)null).First() == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new string[] { \"b\", \"a\" }.OrderBy(x => null, StringComparer.Ordinal).First() == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrStringOrderingPreservesExplicitAndParenthesizedNullKeys(Options options, string expression)
        {
            AssertPersistedStringOrderingExpression(options, expression);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrStringOrderingPreservesCultureComparerDuringIndexing(Options options)
        {
            AssertPersistedStringOrderingExpression(options, "new string[] { \"B\", \"a\" }.OrderBy(x => x, StringComparer.InvariantCulture).First() == \"a\"");
        }

        private void AssertPersistedStringOrderingExpression(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ClrStringLambdaBinding";
            string map = "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            Assert.Equal(map, Assert.Single(store.Maintenance.Send(new GetIndexesOperation(0, 10)).Single().Maps));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Values = new[] { 1L } });
                session.Store(new PrimitiveArrayItem { Values = new[] { 2L } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.Equal(new[] { true, true }, session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").ToList().Select(row => row.Found).ToArray());
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("forward", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("reverse", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("instance", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("forward", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("reverse", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("instance", true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualPreservesClrCharacterAndStringDistinctionDuringIndexing(Options options, string direction, bool sameType)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ClrSequenceTextOrigin";
            const string values = "item.Values.Select(value => (char)value)";
            string other = sameType ? "new object[] { 'a', 'b' }" : "new object[] { \"a\", \"b\" }";
            string expression = direction == "instance" ? "new DynamicArray(" + values + ").SequenceEqual(" + other + ")" : direction == "reverse" ? "Enumerable.SequenceEqual((IEnumerable<object>)" + other + ", (IEnumerable<object>)" + values + ")" : "Enumerable.SequenceEqual((IEnumerable<object>)" + values + ", (IEnumerable<object>)" + other + ")";
            string map = "from item in docs.ContainsOriginItems select new { item.Key, Found = " + expression + ", Types = " + values + ".Select(value => value.GetType().Name).ToArray() }";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Types"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            Assert.Equal(map, Assert.Single(store.Maintenance.Send(new GetIndexesOperation(0, 10)).Single().Maps));
            using (var session = store.OpenSession())
            {
                session.Store(new ContainsOriginItem { Key = "hit", Values = new[] { 97L, 98L } });
                session.Store(new ContainsOriginItem { Key = "miss", Values = new[] { 99L, 100L } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                ContainsOriginResult[] rows = session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found, Types").ToList().ToArray();
                Assert.All(rows.SelectMany(row => row.Types), type => Assert.Equal("Char", type));
                Assert.Equal(new[] { ("hit", sameType), ("miss", false) }, rows.OrderBy(row => row.Key).Select(row => (row.Key, row.Found)).ToArray());
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.SequenceEqual(first: new long[] { 1L, 2L }, second: new long[] { 1L, 2L })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(second: new long[] { 1L, 2L }, first: new long[] { 1L, 2L })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new long[] { 1L, 2L }, second: new long[] { 1L, 2L })", SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualPreservesNamedArgumentBindingDuringIndexing(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/NamedSequenceArguments";
            string map = "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            Assert.Equal(map, Assert.Single(store.Maintenance.Send(new GetIndexesOperation(0, 10)).Single().Maps));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Values = new[] { 1L, 2L } });
                session.Store(new PrimitiveArrayItem { Values = new[] { 3L } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.Equal(new[] { true, true }, session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").ToList().Select(row => row.Found).ToArray());
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("year", "Enumerable.Contains((IEnumerable<object>)item.Dates.Select(date => DateTime.Parse(date.ToString()).Year), (object)2024)", "item.Dates.Select(date => DateTime.Parse(date.ToString()).Year)", "Int32", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("length", "Enumerable.Contains((IEnumerable<object>)item.Names.Select(name => name.Length), (object)5)", "item.Names.Select(name => name.Length)", "Int32", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("count", "Enumerable.Contains((IEnumerable<object>)item.Groups.Select(values => values.Count()), (object)5)", "item.Groups.Select(values => values.Count())", "Int32", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("conditional", "Enumerable.Contains((IEnumerable<object>)item.Values.Select(value => value > 0 ? (int)value : 0), (object)(int)item.Target)", "item.Values.Select(value => value > 0 ? (int)value : 0)", "Int32", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("grouping", "item.Names.Select(name => name.Length).GroupBy(length => length).Any(grouped => grouped.Contains(5))", "item.Names.Select(name => name.Length)", "Int32", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("short", "Enumerable.Contains((IEnumerable<object>)item.Values.Select(value => (short)value), (object)(short)item.Target)", "item.Values.Select(value => (short)value)", "Int16", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("uint", "Enumerable.Contains((IEnumerable<object>)item.Values.Select(value => (uint)value), (object)(uint)item.Target)", "item.Values.Select(value => (uint)value)", "UInt32", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("ulong", "Enumerable.Contains((IEnumerable<object>)item.Values.Select(value => (ulong)value), (object)(ulong)item.Target)", "item.Values.Select(value => (ulong)value)", "UInt64", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("float", "Enumerable.Contains((IEnumerable<object>)item.Values.Select(value => (float)value), (object)(float)item.Target)", "item.Values.Select(value => (float)value)", "Single", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("large-ulong", "Enumerable.Contains((IEnumerable<object>)item.Values.Select(value => (ulong)value + (ulong)long.MaxValue), (object)((ulong)item.Target + (ulong)long.MaxValue))", "item.Values.Select(value => (ulong)value + (ulong)long.MaxValue)", "UInt64", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesComputedPrimitiveOrigin(Options options, string scenario, string expression, string values, string expectedType)
        {
            using var store = GetDocumentStore(options);
            string indexName = "PrimitiveArrays/ComputedOrigin/" + scenario;
            string map = "from item in docs.ContainsOriginItems select new { item.Key, Found = " + expression + ", Types = " + values + ".Select(value => value.GetType().Name).ToArray() }";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Types"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(ContainsOriginItem.Create(true));
                session.Store(ContainsOriginItem.Create(false));
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Equal(map, Assert.Single(store.Maintenance.Send(new GetIndexesOperation(0, 10)).Single().Maps));
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                ContainsOriginResult[] rows = session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found, Types").ToList().ToArray();
                Assert.Equal(2, rows.Length);
                Assert.All(rows.SelectMany(row => row.Types), type => Assert.Equal(expectedType, type));
                Assert.Equal(new[] { ("hit", true), ("miss", false) }, rows.OrderBy(row => row.Key).Select(row => (row.Key, row.Found)).ToArray());
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsFindsAppendedClrValues(Options options, bool reverse)
        {
            using var store = GetDocumentStore(options);
            var index = new AppendedClrContainsIndex(reverse);
            store.ExecuteIndex(index);
            string map = Assert.Single(index.CreateIndexDefinition().Maps);
            Assert.Contains("DynamicEnumerable.Concat", map);
            Assert.Contains("Enumerable.Contains", map);
            Assert.Equal(map, Assert.Single(store.Maintenance.Send(new GetIndexesOperation(0, 10)).Single().Maps));
            using (var session = store.OpenSession())
            {
                session.Store(new ContainsOriginItem { Key = "appended", Values = new[] { 4L, 6L }, Target = 5 });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.True(session.Advanced.RawQuery<ContainsOriginResult>($"from index '{index.IndexName}' select Found").Single().Found);
        }

        private sealed class AppendedClrContainsIndex : AbstractIndexCreationTask<ContainsOriginItem, ContainsOriginResult>
        {
            public AppendedClrContainsIndex(bool reverse)
            {
                if (reverse)
                    Map = items => from item in items
                                   select new ContainsOriginResult { Found = new object[] { (int)item.Target }.Concat(item.Values.Cast<object>()).Contains((int)item.Target) };
                else
                    Map = items => from item in items
                                   select new ContainsOriginResult { Found = item.Values.Cast<object>().Concat(new object[] { (int)item.Target }).Contains((int)item.Target) };
                Store(result => result.Found, FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesComputedValuesAfterMapReduceStorage(Options options)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ComputedOriginReduce";
            const string map = "from item in docs.ContainsOriginItems select new { item.Key, item.Names, Found = false, Types = new string[0] }";
            const string reduce = "from result in results group result by result.Key into grouped let names = grouped.First().Names select new { Key = grouped.Key, Names = names, Found = Enumerable.Contains((IEnumerable<object>)names.Select(name => name.Length), (object)5), Types = new[] { names.GetType().Name, names.Select(name => name.Length).First().GetType().Name } }";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Reduce = reduce,
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Types"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(ContainsOriginItem.Create(true));
                session.Store(ContainsOriginItem.Create(false));
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            IndexDefinition persisted = store.Maintenance.Send(new GetIndexesOperation(0, 10)).Single();
            Assert.Equal(map, Assert.Single(persisted.Maps));
            Assert.Equal(reduce, persisted.Reduce);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                ContainsOriginResult[] rows = session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found, Types").ToList().ToArray();
                Assert.All(rows, row => Assert.Equal(new[] { "DynamicArray", "Int32" }, row.Types));
                Assert.Equal(new[] { ("hit", true), ("miss", false) }, rows.OrderBy(row => row.Key).Select(row => (row.Key, row.Found)).ToArray());
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("int", "default", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("int", "create", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("int", "null", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("int", "default", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("int", "create", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("int", "null", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("float", "default", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("float", "create", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "default", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "create", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "null", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "default", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "create", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("char", "null", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("string", "default", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("string", "create", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("string", "null", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("string", "default", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("string", "create", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("string", "null", true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesGenericComparerOriginDuringIndexing(Options options, string type, string form, bool receiver)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ComparerOrigin";
            string values = type == "string" ? "item.Names.Select(name => name.ToString().Substring(0, 1))" : "item.Values.Select(value => (" + type + ")value)";
            string needle = type is "char" or "string" ? "'a'" : "(" + type + ")item.Target";
            string comparer = form == "default" ? "EqualityComparer<object>.Default" : form == "create" ? "EqualityComparer<object>.Create((left, right) => object.Equals(left, right))" : receiver ? "null" : "(IEqualityComparer<object>)null";
            string expression = receiver ? "new DynamicArray(" + values + ").Contains(" + needle + ", " + comparer + ")" : "Enumerable.Contains((IEnumerable<object>)" + values + ", (object)(" + needle + "), " + comparer + ")";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ContainsOriginItems select new { item.Key, Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                ContainsOriginItem hit = ContainsOriginItem.Create(true), miss = ContainsOriginItem.Create(false);
                if (type is "char" or "string")
                {
                    hit.Values = new[] { 97L, 98L }; miss.Values = new[] { 99L, 100L };
                }
                session.Store(hit); session.Store(miss); session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.Equal(new[] { ("hit", type != "string"), ("miss", false) }, session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found").ToList().OrderBy(row => row.Key).Select(row => (row.Key, row.Found)).ToArray());
        }

        private sealed class ContainsOriginItem
        {
            public string Key { get; set; }
            public long[] Values { get; set; }
            public long Target { get; set; }
            public string[] Names { get; set; }
            public DateTime[] Dates { get; set; }
            public long[][] Groups { get; set; }
            public static ContainsOriginItem Create(bool hit)
            {
                return new ContainsOriginItem { Key = hit ? "hit" : "miss", Values = hit ? new[] { 5L } : new[] { 4L, 6L }, Target = 5, Names = hit ? new[] { "aaaaa" } : new[] { "bbb", "cccc" }, Dates = new[] { new DateTime(hit ? 2024 : 2023, 1, 1) }, Groups = new[] { new long[hit ? 5 : 4] } };
            }
        }

        private sealed class ContainsOriginResult
        {
            public string Key { get; set; }
            public bool Found { get; set; }
            public string[] Types { get; set; }
            public string[] Trace { get; set; }
        }
        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("equals-static", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("equals-instance", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("equals-character", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("comparer-static", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("comparer-receiver", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("comparer-generic-only", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("collection-static", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("collection-instance", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("collection-null", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("collection-default", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("collection-custom", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesCallbackAndCollectionOriginsDuringIndexing(Options options, string scenario)
        {
            using var store = GetDocumentStore(options);
            string indexName = "PrimitiveArrays/CallbackOrigin/" + scenario;
            string lets, expression, trace;
            (string Key, bool Found, string Trace)[] expected;
            if (scenario.StartsWith("equals"))
            {
                bool character = scenario == "equals-character";
                lets = "let values = new CustomValues(item.Values, false, " + character.ToString().ToLowerInvariant() + ")";
                expression = scenario == "equals-instance" ? "values.Contains(5)" : "Enumerable.Contains((IEnumerable<object>)values, (object)" + (character ? "'a'" : "5") + ")";
                trace = "values.Trace.ToArray()";
                expected = character
                    ? new[] { ("hit", true, "97:Char:a"), ("miss", false, "99:Char:a|100:Char:a") }
                    : scenario == "equals-instance"
                        ? new[] { ("hit", false, "5:Int64:5"), ("miss", false, "4:Int64:5|6:Int64:5") }
                        : new[] { ("hit", true, "5:Int32:5"), ("miss", false, "4:Int32:5|6:Int32:5") };
            }
            else if (scenario.StartsWith("comparer"))
            {
                lets = "let comparer = new " + (scenario == "comparer-generic-only" ? "GenericIntNeedleComparer" : "IntNeedleComparer") + "()";
                expression = scenario == "comparer-receiver" ? "new DynamicArray(item.Values).Contains(5, comparer)" : "Enumerable.Contains((IEnumerable<object>)item.Values, (object)5, (IEqualityComparer<object>)comparer)";
                trace = "comparer.Trace.ToArray()";
                expected = new[] { ("hit", true, "Int64/Int32:5/5"), ("miss", false, "Int64/Int32:4/5|Int64/Int32:6/5") };
            }
            else
            {
                lets = "let values = new OriginCollection(item.Values, " + (scenario == "collection-null" ? "true" : "false") + ")";
                expression = scenario switch
                {
                    "collection-instance" => "values.Contains(5)",
                    "collection-null" => "Enumerable.Contains((IEnumerable<object>)values, (object)99, (IEqualityComparer<object>)null)",
                    "collection-default" => "Enumerable.Contains((IEnumerable<object>)values, (object)5, EqualityComparer<object>.Default)",
                    "collection-custom" => "Enumerable.Contains((IEnumerable<object>)values, (object)5, EqualityComparer<object>.Create((left, right) => object.Equals(left, right)))",
                    _ => "Enumerable.Contains((IEnumerable<object>)values, (object)5)"
                };
                trace = "values.Trace.ToArray()";
                expected = scenario == "collection-instance"
                    ? new[] { ("hit", false, "collection.Contains:Int64:5"), ("miss", false, "collection.Contains:Int64:5") }
                    : scenario == "collection-static"
                        ? new[] { ("hit", true, "collection.Contains:Int32:5"), ("miss", false, "collection.Contains:Int32:5") }
                        : new[] { ("hit", scenario != "collection-null", "enumerate"), ("miss", false, "enumerate") };
            }
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ContainsOriginItems " + lets + " select new { item.Key, Found = " + expression + ", Trace = " + trace + " }" },
                AdditionalSources = { ["ContainsOrigins.cs"] = ContainsOriginSources },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Trace"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                ContainsOriginItem hit = ContainsOriginItem.Create(true), miss = ContainsOriginItem.Create(false);
                if (scenario == "equals-character") { hit.Values = new[] { 97L, 98L }; miss.Values = new[] { 99L, 100L }; }
                session.Store(hit); session.Store(miss); session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.Equal(expected, session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found, Trace").ToList().OrderBy(row => row.Key).Select(row => (row.Key, row.Found, string.Join("|", row.Trace))).ToArray());
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesRawEqualityEarlyExitAndThrowDuringIndexing(Options options, bool miss)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/RawEqualityEarlyExit";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ContainsOriginItems let values = new CustomValues(item.Values, true, false) select new { Found = Enumerable.Contains((IEnumerable<object>)values, (object)5), Trace = values.Trace.ToArray() }" },
                AdditionalSources = { ["ContainsOrigins.cs"] = ContainsOriginSources },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Trace"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new ContainsOriginItem { Key = miss ? "miss" : "hit", Values = miss ? new[] { 4L, 6L } : new[] { 5L, 6L } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            if (miss)
            {
                IndexErrors[] errors = Indexes.WaitForIndexingErrors(store, new[] { indexName }, errorsShouldExists: true);
                Assert.NotNull(errors);
                IndexingError error = Assert.Single(Assert.Single(errors).Errors);
                Assert.Contains("custom-equals-after-first:4:Int32:5|6:Int32:5", error.Error);
            }
            else
            {
                Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
                using (var session = store.OpenSession())
                {
                    ContainsOriginResult row = session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Found, Trace").Single();
                    Assert.Equal((true, "5:Int32:5"), (row.Found, string.Join("|", row.Trace)));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesCharacterCallOriginDuringIndexing(Options options, bool instance, bool stored)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/CharacterOrigin";
            string values = stored ? "item.Names" : "item.Names.Select(name => name.ToString().Substring(0, 1))";
            string expression = instance ? "new DynamicArray(" + values + ").Contains('a')" : "Enumerable.Contains((IEnumerable<object>)" + values + ", (object)'a')";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ContainsOriginItems select new { item.Key, Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new ContainsOriginItem { Key = "hit", Names = new[] { "a" } });
                session.Store(new ContainsOriginItem { Key = "miss", Names = new[] { "b" } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.Equal(new[] { ("hit", instance || stored), ("miss", false) }, session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found").ToList().OrderBy(row => row.Key).Select(row => (row.Key, row.Found)).ToArray());
        }

        private const string ContainsOriginSources = """
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Indexes.Static;
public sealed class IntNeedleComparer : IEqualityComparer<object>, IEqualityComparer
{
    public readonly List<string> Trace = new List<string>();
    public bool Equals(object left, object right)
    {
        Trace.Add((left == null ? "null" : left.GetType().Name) + "/" + (right == null ? "null" : right.GetType().Name) + ":" + left + "/" + right);
        return left is long number && right is int needle && number == needle;
    }
    int IEqualityComparer<object>.GetHashCode(object value) => 0;
    int IEqualityComparer.GetHashCode(object value) => 0;
}
public sealed class GenericIntNeedleComparer : IEqualityComparer<object>
{
    public readonly List<string> Trace = new List<string>();
    public bool Equals(object left, object right)
    {
        Trace.Add((left == null ? "null" : left.GetType().Name) + "/" + (right == null ? "null" : right.GetType().Name) + ":" + left + "/" + right);
        return left is long number && right is int needle && number == needle;
    }
    int IEqualityComparer<object>.GetHashCode(object value) => 0;
}
public sealed class CustomValues : DynamicArray
{
    public readonly List<string> Trace;
    public CustomValues(IEnumerable values, bool throwOnSecond, bool character) : this(values, throwOnSecond, character, new List<string>()) { }
    private CustomValues(IEnumerable values, bool throwOnSecond, bool character, List<string> trace) : base(Make(values, throwOnSecond, character, trace)) { Trace = trace; }
    private static IEnumerable<object> Make(IEnumerable values, bool throwOnSecond, bool character, List<string> trace)
    {
        int ordinal = 0;
        foreach (object number in values)
            yield return new RawNeedleValue(Convert.ToInt32(number), throwOnSecond && ordinal++ > 0, character, trace);
    }
    private sealed class RawNeedleValue
    {
        private readonly int _value;
        private readonly bool _throw, _character;
        private readonly List<string> _trace;
        public RawNeedleValue(int value, bool throwing, bool character, List<string> trace) { _value = value; _throw = throwing; _character = character; _trace = trace; }
        public override bool Equals(object other)
        {
            _trace.Add(_value + ":" + (other == null ? "null" : other.GetType().Name) + ":" + other);
            if (_throw) throw new InvalidOperationException("custom-equals-after-first:" + string.Join("|", _trace));
            return _character ? other is char ch && ch == _value : other is int number && number == _value;
        }

    }
}
public sealed class OriginCollection : DynamicArray, ICollection<object>
{
    private readonly object[] _values;
    private readonly bool _always;
    public readonly List<string> Trace = new List<string>();
    public OriginCollection(IEnumerable values, bool always) : this(values.Cast<object>().Select(value => (object)Convert.ToInt32(value)).ToArray(), always) { }
    private OriginCollection(object[] values, bool always) : base(values) { _values = values; _always = always; }
    bool ICollection<object>.Contains(object item)
    {
        Trace.Add("collection.Contains:" + (item == null ? "null" : item.GetType().Name) + ":" + item);
        return _always || _values.Contains(item);
    }
    IEnumerator<object> IEnumerable<object>.GetEnumerator()
    {
        Trace.Add("enumerate");
        foreach (object value in _values) yield return value;
    }
    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<object>)this).GetEnumerator();
    public int Count => _values.Length;
    public bool IsReadOnly => true;
    void ICollection<object>.Add(object item) => throw new NotSupportedException();
    void ICollection<object>.Clear() => throw new NotSupportedException();
    bool ICollection<object>.Remove(object item) => throw new NotSupportedException();
    void ICollection<object>.CopyTo(object[] array, int index) => _values.CopyTo(array, index);
}
""";

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void PrimitiveArrayOperationsInsideCountPredicateExecute(Options options, bool sequenceEqual)
        {
            using var store = GetDocumentStore(options);
            var index = new PrimitiveArrayCountPredicateIndex(sequenceEqual);
            store.ExecuteIndex(index);
            var map = Assert.Single(index.CreateIndexDefinition().Maps);
            Assert.Contains("Enumerable.Count", map);
            Assert.Contains(sequenceEqual ? "Enumerable.SequenceEqual" : "Enumerable.Contains", map);

            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Values = new[] { 1L, 2L }, Tags = new[] { "a", "b" } });
                session.Store(new PrimitiveArrayItem { Values = new[] { 3L }, Tags = new[] { "c" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors;
            Assert.Empty(errors);
            using (var session = store.OpenSession())
            {
                var counts = session.Advanced.RawQuery<PrimitiveArrayCountPredicateIndex.Result>(
                    $"from index '{index.IndexName}' select Count").ToList();
                Assert.Equal(new[] { 0, 2 }, counts.Select(x => x.Count).OrderBy(x => x));
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void NestedPrimitiveArrayCallsPreserveResultsInMapAndReduce(Options options, bool reduce)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/NestedCalls";
            const string expression = "Enumerable.Contains(new[] { Enumerable.SequenceEqual(/* stored values */ item.Values, new long[] { 1L, 2L }) }, " +
                                      "Enumerable.Contains(Enumerable.Range(0, 3).Select(x => (long)x), 2L /* target */))";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ArrayEnumerationItems select new { item.Group, item.Values, Found = " + (reduce ? "true" : expression) + " }" },
                Reduce = reduce
                    ? "from result in results group result by result.Group into grouped let item = grouped.First() select new { Group = grouped.Key, item.Values, Found = " + expression + " }"
                    : null,
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new ArrayEnumerationItem { Group = "match", Values = new object[] { 1L, 2L } });
                session.Store(new ArrayEnumerationItem { Group = "mismatch", Values = new object[] { 3L } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                bool[] results = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").ToList()
                    .Select(x => x.Found).OrderBy(x => x).ToArray();
                Assert.Equal(new[] { false, true }, results);
            }
        }

        private sealed class PrimitiveArrayCountPredicateIndex : AbstractIndexCreationTask<PrimitiveArrayItem, PrimitiveArrayCountPredicateIndex.Result>
        {
            public PrimitiveArrayCountPredicateIndex(bool sequenceEqual)
            {
                if (sequenceEqual)
                    Map = items => from item in items
                                   select new Result { Count = item.Tags.Count(tag => item.Values.SequenceEqual(new[] { 1L, 2L })) };
                else
                    Map = items => from item in items
                                   select new Result { Count = item.Tags.Count(tag => item.Values.Contains(2L)) };

                Store(x => x.Count, FieldStorage.Yes);
            }

            public sealed class Result
            {
                public int Count { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Contains(new[] { 1, 2, 3 }, (int)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 'a', 'b' }, item.Tags[0].ToString()[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 1f, 2f, 3f }, (float)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { \"2024-01-01T00:00:00.0000000Z\" }, \"2024-01-01T00:00:00.0000000Z\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { \"2024-01-01T00:00:00.0000000Z\" }, \"2024-01-01T00:00:00.0000000Z\", StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 1, 2, 3 }, (int)item.Values[0], EqualityComparer<int>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable/* receiver */.Contains(new[] { 1, 2, 3 }, (int)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable./* method */Contains(new[] { 1, 2, 3 }, (int)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains<int>(new[] { 1, 2, 3 }, (int)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("System.Linq.Enumerable.Contains(new[] { 1, 2, 3 }, (int)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("@Enumerable.Contains(new[] { 1, 2, 3 }, (int)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.@Contains(new[] { 1, 2, 3 }, (int)item.Values[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { \"a\", \"b\" }, item.Tags[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 'a', 'b' }, 'b', EqualityComparer<char>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsPreservesClrArrayValues(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ClrContains";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));

            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Values = new[] { 2L }, Tags = new[] { "a" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.Empty(errors);
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single();
                Assert.True(result.Found, expression);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("!Enumerable.Contains(new char[0], 'a', (IEqualityComparer<char>)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 'a' }, 'a', (IEqualityComparer<char>)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.Contains(new[] { 'a' }, 'b', EqualityComparer<char>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 'a', 'b', 'c' }, 'a', EqualityComparer<char>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 'a', 'b', 'c' }, 'c', (IEqualityComparer<char>)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 'a', 'B' }, 'b', EqualityComparer<char>.Create((char x, char y) => char.ToUpperInvariant(x) == char.ToUpperInvariant(y), (char x) => 0))", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains((IEnumerable<char>)new[] { 'a', 'b' }, 'b', EqualityComparer<char>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 1L, 2L }, 2L, EqualityComparer<long>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.Contains(new object[] { \"a\" }, 'a', (IEqualityComparer<object>)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharArrayContainsPreservesComparerDuringIndexing(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Letters = new[] { 'a', 'B' } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false)]
        [InlineData(true)]
        public void CharArrayContainsPreservesComparerCallsAndExceptions(bool throwOnSecond)
        {
            var expectedTrace = new List<char>();
            var actualTrace = new List<char>();
            bool? Run(bool production, List<char> trace)
            {
                char[] values = { 'a', 'b', 'c' };
                var comparer = EqualityComparer<char>.Create((x, y) =>
                {
                    trace.Add(x);
                    values[1] = 'z';
                    if (throwOnSecond && trace.Count == 2)
                        throw new InvalidOperationException("comparer");
                    return x == y;
                }, _ => 0);
                return production
                    ? DynamicEnumerable.Contains(values, 'z', comparer)
                    : Enumerable.Contains(values, 'z', comparer);
            }

            bool? expected = null;
            bool? actual = null;
            var expectedException = Record.Exception(() => expected = Run(false, expectedTrace));
            var actualException = Record.Exception(() => actual = Run(true, actualTrace));
            Assert.Equal(expected, actual);
            Assert.Equal(expectedException?.GetType(), actualException?.GetType());
            Assert.Equal(expectedException?.Message, actualException?.Message);
            Assert.Equal(expectedTrace, actualTrace);
            Assert.Equal("source", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.Contains<char>(null, 'a', null)).ParamName);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("(int)int.MinValue", 0, 0, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(short)short.MinValue", 1, 7, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(ushort)ushort.MaxValue", 2, 2, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(uint)uint.MaxValue", 3, 3, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(ulong)long.MaxValue", 4, 4, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(ulong)long.MaxValue + 1UL", -1, -1, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("ulong.MaxValue", -1, -1, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(byte)1", -1, -1, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(sbyte)1", -1, -1, SearchEngineMode = RavenSearchEngineMode.All)]
        public void StoredArrayPrimitiveTargetConversionsRemainUnchanged(Options options, string target, int first, int last)
        {
            string expression = $"item.Values.Contains({target}) == {(first >= 0 ? "true" : "false")} && " +
                                $"item.Values.IndexOf({target}) == {first} && item.Values.LastIndexOf({target}) == {last}";
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem
            {
                Values = new[] { (long)int.MinValue, short.MinValue, ushort.MaxValue, uint.MaxValue, long.MaxValue, 0L, 1L, short.MinValue }
            });
        }

        private sealed class ClrContainsResult
        {
            public bool Found { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"2024-01-01T00:00:00.0000000Z\" }, new[] { \"2024-01-01T00:00:00.0000000Z\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new object[] { null }, new object[] { null })", SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualPreservesClrStringsAndNulls(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.SequenceEqual(new object[0], new object[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(new object[0], new object[] { null })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(new object[] { null }, new object[0])", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(new object[] { 1L, 2L }, new object[] { 3L, 2L })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(new object[] { 1L, 2L }, new object[] { 1L, 3L })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(item.Tags.ToArray(), item.Tags.ToArray())", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"a\", null }, new object[] { \"a\", null })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { 1L, 2L }, new[] { 1L, 2L })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual((IEnumerable<object>)new object[] { null, 1L }, new object[] { null, 1L })", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ArraySequenceEqualPreservesBoundariesDuringIndexing(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", null, "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false)]
        [InlineData(true)]
        public void ArraySequenceEqualPreservesCustomEquality(bool nonGeneric)
        {
            bool Equal(object[] first, object[] second) => nonGeneric
                ? DynamicEnumerable.SequenceEqual((object)first, second)
                : DynamicEnumerable.SequenceEqual(first, second);

            object[] strings = new string[] { "a", null };
            Assert.True(Equal(strings, new object[] { "a", null }));
            int calls = 0;
            object[] values = new EqualityCallbackValue[] { new(_ => { calls++; return false; }) };
            Assert.False(Equal(values, values));
            Assert.Equal(1, calls);
            Assert.False(Equal(values, Array.Empty<object>()));
            Assert.Equal(1, calls);

            object[] throwing = new EqualityCallbackValue[] { new(_ => throw new InvalidOperationException("equality")) };
            Assert.Equal("equality", Assert.Throws<InvalidOperationException>(() => Equal(throwing, throwing)).Message);
            Assert.False(Equal(new object[] { 1L }, new object[] { 1 }));
            Assert.False(Equal(new object[] { 'a' }, new object[] { "a" }));

            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.SequenceEqual<object>(null, Array.Empty<object>())).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.SequenceEqual(Array.Empty<object>(), (object[])null)).ParamName);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Contains(item.Tags, null, StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("item.Tags.OrderBy(x => x, StringComparer.Ordinal).First() == null", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("item.Tags.OrderByDescending(x => x, StringComparer.Ordinal).Last() == null", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags, null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags, null, (IEqualityComparer)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ComparersAcceptNullArrayEntries(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", null, "b" } });
        }

        private void AssertPrimitiveArrayExpression(Options options, string expression, PrimitiveArrayItem item)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/Expression";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(item);
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.Empty(errors);
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single();
                Assert.True(result.Found, expression);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrderingComparersExecuteInReduce(Options options, bool descending)
        {
            using var store = GetDocumentStore(options);
            var index = new OrderingComparerReduceIndex(descending);
            store.ExecuteIndex(index);
            Assert.Contains(descending ? "DynamicEnumerable.OrderByDescending" : "DynamicEnumerable.OrderBy", index.CreateIndexDefinition().Reduce);
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Group = "one", Tags = new[] { "b", "A" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors;
            Assert.Empty(errors);
            using (var session = store.OpenSession())
            {
                var result = session.Query<OrderingComparerReduceIndex.Result>(index.IndexName).Single();
                Assert.Equal(descending ? "b" : "A", result.First);
            }
        }

        private sealed class OrderingComparerReduceIndex : AbstractIndexCreationTask<PrimitiveArrayItem, OrderingComparerReduceIndex.Result>
        {
            public OrderingComparerReduceIndex(bool descending)
            {
                Map = items => from item in items select new Result { Group = item.Group, Tags = item.Tags, First = null };
                if (descending)
                    Reduce = results => from result in results
                                        group result by result.Group into grouped
                                        select new Result
                                        {
                                            Group = grouped.Key,
                                            Tags = grouped.First().Tags,
                                            First = grouped.First().Tags.OrderByDescending(x => x, StringComparer.OrdinalIgnoreCase).First()
                                        };
                else
                    Reduce = results => from result in results
                                        group result by result.Group into grouped
                                        select new Result
                                        {
                                            Group = grouped.Key,
                                            Tags = grouped.First().Tags,
                                            First = grouped.First().Tags.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).First()
                                        };
            }

            public sealed class Result
            {
                public string Group { get; set; }
                public string[] Tags { get; set; }
                public string First { get; set; }
            }
        }


        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Distinct(new object[] { Enumerable.Contains(item.Values, 2) }).First()", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Range(0, Enumerable.Contains(item.Values, 2) ? 1 : 0).Select(x => x).Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Range(0, Enumerable.SequenceEqual(item.Values, new[] { 1L, 2L }) ? 1 : 0).Select(x => x).Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        public void PrimitiveArrayOperationsInsideRewrittenCallsExecute(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Values = new[] { 1L, 2L } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Concat(new[] { \"2024-01-01T00:00:00.0000000Z\" }, new[] { \"tail\" }).First() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 1, 2, 3 }, (int)item.Values[0], (IEqualityComparer<int>)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new[] { 1, 2 }, new[] { 3 }).Count() == 3", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new object[] { \"2024-01-01T00:00:00.0000000Z\" }, new object[] { \"tail\" }).First() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(first: new[] { 1L }, second: new[] { 2L }).Last() == 2L", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(second: new[] { 2L }, first: new[] { 1L }).Last() == 2L", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat<long>(new[] { 1L }, new[] { 2L }).Last() == 2L", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrEnumerableOperationsPreserveValues(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Values = new[] { 2L } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("DynamicEnumerable.Concat(new[] { \"prefix\" }, item.Tags).Where(x => x == \"prefix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Concat(new[] { \"prefix\" }, item.Tags).Select(x => x).First() == \"prefix\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Concat(item.Tags, new[] { \"suffix\" }).Where(x => x == \"suffix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Concat(new[] { \"prefix\" }, item.Tags.ToArray()).Where(x => x == \"prefix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new[] { \"prefix\" }, (IEnumerable<dynamic>)item.Tags).Where(x => x == \"prefix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ConcatSupportsChainedOperationsWithStoredArrays(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Contains((IEnumerable)new[] { 1, 2, 3 }, 2)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains((IEnumerable)new[] { 'a', 'b' }, 'b')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains((IEnumerable)new[] { \"2024-01-01T00:00:00.0000000Z\" }, \"2024-01-01T00:00:00.0000000Z\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains((IEnumerable)new[] { 1, 2, 3 }, 2, (IEqualityComparer)EqualityComparer<int>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual((IEnumerable)new[] { \"2024-01-01T00:00:00.0000000Z\" }, (IEnumerable)new[] { \"2024-01-01T00:00:00.0000000Z\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual((IEnumerable)new object[] { null }, (IEnumerable)new object[] { null })", SearchEngineMode = RavenSearchEngineMode.All)]
        public void NonGenericEnumerableOperationsPreserveClrValues(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Contains(DynamicEnumerable.Concat(item.Letters, new[] { 'c' }), 'c')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Concat(item.Letters, new[] { 'c' }).Contains('c')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(DynamicEnumerable.Concat(item.Letters, new[] { 'c' }), new[] { 'a', 'b', 'c' })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Letters, 'b')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Concat(item.Values, new[] { 3L }).Contains(3L)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(item.Values, new[] { 3L }).Contains(3L)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new[] { 3L }, item.Values).Contains(3L)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ConcatPreservesCharacterMembership(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression,
                new PrimitiveArrayItem { Letters = new[] { 'a', 'b' }, Values = new[] { 1L, 2L } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterContainsPreservesDerivedEnumerationDuringIndexing(Options options)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/DerivedCharacterContains";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems let tail = new TailArray(item.Tags) select new { ContainsRemoved = tail.Contains('a'), ContainsKept = tail.Contains('b') }" },
                Fields =
                {
                    ["ContainsRemoved"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["ContainsKept"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
                },
                AdditionalSources = new Dictionary<string, string> { ["TailArray.cs"] = """
                    using System.Collections;
                    using System.Collections.Generic;
                    using System.Linq;
                    using Raven.Server.Documents.Indexes.Static;

                    public sealed class TailArray : DynamicArray, IEnumerable<object>
                    {
                        private readonly IEnumerable<object> _values;
                        public TailArray(IEnumerable<object> values) : base(values) { _values = values; }
                        IEnumerator<object> IEnumerable<object>.GetEnumerator() => _values.Skip(1).GetEnumerator();
                        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<object>)this).GetEnumerator();
                    }
                    """ }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                DerivedCharacterContainsResult result = session.Advanced.RawQuery<DerivedCharacterContainsResult>(
                    $"from index '{indexName}' select ContainsRemoved, ContainsKept").Single();
                Assert.False(result.ContainsRemoved, "The derived interface enumerator excludes 'a'.");
                Assert.True(result.ContainsKept, "The derived interface enumerator retains 'b'.");
            }
        }

        private sealed class DerivedCharacterContainsResult
        {
            public bool ContainsRemoved { get; set; }
            public bool ContainsKept { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterContainsPreservesGroupedClrValues(Options options)
        {
            using var store = GetDocumentStore(options);
            GroupedCharacterContainsIndex index = new GroupedCharacterContainsIndex();
            store.ExecuteIndex(index);
            string map = Assert.Single(index.CreateIndexDefinition().Maps);
            Assert.Contains("Enumerable.Contains", map);
            Assert.Equal(map, Assert.Single(store.Maintenance.Send(new GetIndexOperation(index.IndexName)).Maps));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Values = new[] { 97L, 98L } });
                session.Store(new PrimitiveArrayItem { Values = new[] { 99L, 100L } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                bool[] results = session.Advanced.RawQuery<ClrContainsResult>($"from index '{index.IndexName}' select Found")
                    .ToList().Select(result => result.Found).OrderBy(found => found).ToArray();
                Assert.Equal(new[] { false, true }, results);
            }
        }

        private sealed class GroupedCharacterContainsIndex : AbstractIndexCreationTask<PrimitiveArrayItem, ClrContainsResult>
        {
            public GroupedCharacterContainsIndex()
            {
                Map = items => from item in items
                               select new ClrContainsResult
                               {
                                   Found = item.Values.Select(value => (char)value).GroupBy(character => character).Any(grouped => grouped.Contains('a'))
                               };
                Store(result => result.Found, FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterContainsPreservesDerivedCollectionDispatchDuringIndexing(Options options)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/CollectionCharacterContains";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems let collection = new CharacterCollection(item.Tags) let found = collection.Contains('a') select new { Found = found, collection.ContainsCalls, collection.NeedleType }" },
                Fields =
                {
                    ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["ContainsCalls"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["NeedleType"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
                },
                AdditionalSources = new Dictionary<string, string> { ["CharacterCollection.cs"] = """
                    using System;
                    using System.Collections;
                    using System.Collections.Generic;
                    using Raven.Server.Documents.Indexes.Static;

                    public sealed class CharacterCollection : DynamicArray, ICollection<object>
                    {
                        private readonly IEnumerable<object> _values;
                        public int ContainsCalls;
                        public string NeedleType;
                        public CharacterCollection(IEnumerable<object> values) : base(values) { _values = values; }
                        bool ICollection<object>.Contains(object item)
                        {
                            ContainsCalls++;
                            NeedleType = item == null ? "null" : item.GetType().Name;
                            return item is string text && text == "a";
                        }
                        IEnumerator<object> IEnumerable<object>.GetEnumerator() => _values.GetEnumerator();
                        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<object>)this).GetEnumerator();
                        public int Count => 1;
                        public bool IsReadOnly => true;
                        void ICollection<object>.Add(object item) => throw new NotSupportedException();
                        void ICollection<object>.Clear() => throw new NotSupportedException();
                        bool ICollection<object>.Remove(object item) => throw new NotSupportedException();
                        void ICollection<object>.CopyTo(object[] array, int index) => throw new NotSupportedException();
                    }
                    """ }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "b" } });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                CollectionCharacterContainsResult result = session.Advanced.RawQuery<CollectionCharacterContainsResult>(
                    $"from index '{indexName}' select Found, ContainsCalls, NeedleType").Single();
                Assert.Equal((true, 1, "String"), (result.Found, result.ContainsCalls, result.NeedleType));
            }
        }

        private sealed class CollectionCharacterContainsResult
        {
            public bool Found { get; set; }
            public int ContainsCalls { get; set; }
            public string NeedleType { get; set; }
        }
        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(0, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(97, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(127, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(128, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(2047, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(2048, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(65535, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterContainsPreservesUtf8Membership(Options options, int character)
        {
            AssertPrimitiveArrayExpression(options,
                $"Enumerable.Contains(item.Tags, (char){character}) && !Enumerable.Contains(item.Tags, '!')",
                new PrimitiveArrayItem { Tags = new[] { null, string.Empty, "ab", "\U0001F985", ((char)character).ToString() } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterContainsPreservesEmptyInput(Options options)
        {
            AssertPrimitiveArrayExpression(options, "!Enumerable.Contains(item.Tags, 'a')",
                new PrimitiveArrayItem { Tags = Array.Empty<string>() });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"a\", null, \"b\" }, item.Tags.ToArray())", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"a\", null, \"b\" }, item.Tags.ToList())", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Letters.ToArray(), 'b')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Letters.ToList(), 'b')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToArray(), null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToList(), null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToArray(), \"b\", StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToList(), \"b\", StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(item.Letters.ToArray(), new[] { 'a', 'b' })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(item.Letters.ToList(), new[] { 'a', 'b' })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(new object[] { 'b' }, new object[] { \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.Contains(new object[] { 'b' }, \"b\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { \"a\", \"b\" }, \"b\", StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Values.ToArray(), 2L)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToArray(), \"B\", StringComparer.OrdinalIgnoreCase)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToList(), \"B\", StringComparer.OrdinalIgnoreCase)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Letters.ToArray(), 'b', (IEqualityComparer)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Letters.ToList(), 'b', (IEqualityComparer<object>)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToArray(), null, StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToList(), null, StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToArray(), null, (IEqualityComparer)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags.ToList(), null, (IEqualityComparer<object>)null)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void MaterializedStoredArraysPreserveTextAndNullEquality(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression,
                new PrimitiveArrayItem { Tags = new[] { "a", null, "b" }, Letters = new[] { 'a', 'b' }, Values = new[] { 1L, 2L } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MaterializedNullContainsPreservesScanBoundaries(Options options)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/NullScanBoundaries";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps =
                {
                    "from item in docs.PrimitiveArrayItems select new { Found = " +
                    "Enumerable.Contains(item.Tags.ToArray(), null) == (item.Group == \"has-null\") && " +
                    "Enumerable.Contains(item.Tags.ToList(), null) == (item.Group == \"has-null\") }"
                },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));

            int count = 0;
            using (var session = store.OpenSession())
            {
                foreach (int length in new[] { 0, 1, 2, 3, 4, 5, 7, 8, 9 })
                {
                    for (int nullPosition = -1; nullPosition < length; nullPosition++)
                    {
                        string[] tags = Enumerable.Repeat("a", length).ToArray();
                        if (nullPosition >= 0)
                            tags[nullPosition] = null;
                        session.Store(new PrimitiveArrayItem { Tags = tags, Group = nullPosition >= 0 ? "has-null" : "no-null" });
                        count++;
                    }
                }
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                var results = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").ToList();
                Assert.Equal(count, results.Count);
                Assert.All(results, result => Assert.True(result.Found));
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("DynamicEnumerable.Min(item.Values) == 1L && DynamicEnumerable.Max(item.Values) == 3L", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Min(new object[0]) == null && DynamicEnumerable.Max(new object[0]) == null", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Min(new object[] { null, 2L, 1L }) == 1L && DynamicEnumerable.Max(new object[] { null, 2L, 1L }) == 2L", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Min(new object[] { null }) == null && DynamicEnumerable.Max(new object[] { null }) == null", SearchEngineMode = RavenSearchEngineMode.All)]
        public void MinAndMaxPreserveIndexedResults(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Values = new[] { 3L, 1L, 2L } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.SequenceEqual(item.Tags, new string[] { \"a\", null, \"b\" })", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"a\", null, \"b\" }, item.Tags)", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"a\", \"b\" }, item.Tags)", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(item.Tags, new[] { \"a\", \"b\" })", false, SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualAcceptsStoredStringsAndNulls(Options options, string expression, bool withNull)
        {
            AssertPrimitiveArrayExpression(options, expression,
                new PrimitiveArrayItem { Tags = withNull ? new[] { "a", null, "b" } : new[] { "a", "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualExecutesReleasedCharArrayDefinition(Options options)
        {
            using var store = GetDocumentStore(options);
            var index = new CharSequenceEqualIndex();
            store.ExecuteIndex(index);
            var map = Assert.Single(index.CreateIndexDefinition().Maps);
            Assert.Contains("Enumerable.SequenceEqual", map);
            Assert.Contains("new char[]", map);
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Letters = new[] { 'a', 'b' } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ClrContainsResult>($"from index '{index.IndexName}' select Found").Single();
                Assert.True(result.Found);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MaterializedStoredArraysExecuteInReduce(Options options)
        {
            using var store = GetDocumentStore(options);
            var index = new MaterializedPrimitiveArrayReduceIndex();
            store.ExecuteIndex(index);
            Assert.Contains("Enumerable.Contains", index.CreateIndexDefinition().Reduce);
            Assert.Contains("Enumerable.SequenceEqual", index.CreateIndexDefinition().Reduce);
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 2; i++)
                    session.Store(new PrimitiveArrayItem { Group = "one", Tags = new[] { "a", null, "b" }, Letters = new[] { 'a', 'b' } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.True(session.Query<MaterializedPrimitiveArrayReduceIndex.Result>(index.IndexName).Single().Found);
        }

        private sealed class MaterializedPrimitiveArrayReduceIndex : AbstractIndexCreationTask<PrimitiveArrayItem, MaterializedPrimitiveArrayReduceIndex.Result>
        {
            public MaterializedPrimitiveArrayReduceIndex()
            {
                Map = items => from item in items select new Result { Group = item.Group, Tags = item.Tags, Letters = item.Letters, Found = false };
                Reduce = results => from result in results
                                    group result by result.Group into grouped
                                    select new Result
                                    {
                                        Group = grouped.Key,
                                        Tags = grouped.First().Tags,
                                        Letters = grouped.First().Letters,
                                        Found = grouped.First().Letters.ToArray().Contains('b') &&
                                                new[] { "a", null, "b" }.SequenceEqual(grouped.First().Tags.ToList()) &&
                                                grouped.First().Tags.ToArray().Contains("B", StringComparer.OrdinalIgnoreCase)
                                    };
            }

            public sealed class Result
            {
                public string Group { get; set; }
                public string[] Tags { get; set; }
                public char[] Letters { get; set; }
                public bool Found { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Contains(item.Letters, 'b', EqualityComparer<char>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Letters.ToArray(), 'b', EqualityComparer<char>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("item.Letters.OrderBy(x => x, Comparer<char>.Default).First().ToString() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(new[] { 'a', 'b' }, 'b', EqualityComparer<char>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterComparersAcceptStoredValues(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Letters = new[] { 'b', 'a' } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrArrayOrderingExecutesClientDefinition(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem());
                session.SaveChanges();
            }

            var index = new ClrArrayOrderingIndex();
            var definition = Assert.Single(index.CreateIndexDefinition().Maps);
            Assert.Contains("new char[]", definition);
            Assert.Contains(".OrderBy(x => x)", definition);
            var compilationError = Record.Exception(() => store.ExecuteIndex(index));
            Assert.Null(compilationError);
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.True(session.Advanced.RawQuery<ClrArrayOrderingIndex.Result>($"from index '{index.IndexName}' select Found").Single().Found);
        }

        private sealed class ClrArrayOrderingIndex : AbstractIndexCreationTask<PrimitiveArrayItem, ClrArrayOrderingIndex.Result>
        {
            public ClrArrayOrderingIndex()
            {
                Map = items => from item in items
                               select new Result { Found = new[] { 'b', 'a' }.OrderBy(x => x).SequenceEqual(new[] { 'a', 'b' }) };
                Store(x => x.Found, FieldStorage.Yes);
            }

            public sealed class Result
            {
                public bool Found { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.SequenceEqual(new[] { 'b', 'a' }.OrderBy(x => x, Comparer<char>.Default), new[] { 'a', 'b' })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new long[] { 2, 1 }.OrderByDescending(x => x, (IComparer<long>)null), new long[] { 2, 1 })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { 2, 1, 3 }.OrderBy(x => x % 2).ThenByDescending(x => x), new[] { 2, 3, 1 })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { 2, 1, 3 }.OrderByDescending(x => x % 2).ThenBy(x => x), new[] { 1, 3, 2 })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"b\", null, \"A\" }.OrderBy(x => x, StringComparer.OrdinalIgnoreCase), new[] { (string)null, \"A\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(Enumerable.OrderBy(new char[] { 'b', 'a' }, x => x, Comparer<char>.Default), new char[] { 'a', 'b' })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new object[] { \"bb\", \"a\" }.OrderBy(x => x.Length), new[] { \"a\", \"bb\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { (object)\"bb\", \"a\" }.OrderBy(x => x.Length), new[] { \"a\", \"bb\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"ab\", \"AA\" }.OrderBy((Func<object, object>)(x => x)).ThenBy(x => x.ToUpperInvariant()), new[] { \"AA\", \"ab\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { 1, -2, 0 }.OrderBy(x => x), new[] { -2, 0, 1 })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { ('b'), ('a') }.OrderBy(x => x), new[] { 'a', 'b' })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { !false, false }.OrderBy(x => x), new[] { false, true })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { 0, ~1 }.OrderBy(x => x), new[] { -2, 0 })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderBy(x => x).ThenBy(x => x, StringComparer.Ordinal), new string[] { null, \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"b\", \"a\" }.OrderByDescending((x) => (x)).ThenByDescending(x => x, StringComparer.OrdinalIgnoreCase), new[] { \"b\", \"a\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(Enumerable.ThenBy(Enumerable.OrderBy(new string[] { \"b\", \"a\" }, x => x), x => x, StringComparer.Ordinal), new[] { \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"b\", \"a\" }.OrderBy((object x) => x), new[] { \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"bb\", \"a\" }.OrderBy(x => x.Length), new[] { \"a\", \"bb\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderBy(x => x, null).ThenBy(x => x, StringComparer.Ordinal), new string[] { null, \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(Enumerable.OrderByDescending(new[] { \"a\", \"b\" }, x => x, null).ThenByDescending(x => x, null), new[] { \"b\", \"a\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"b\", \"a\" }.OrderBy(x => x.ToUpperInvariant()), new[] { \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"b\", \"a\" }.OrderByDescending(x => x.ToUpperInvariant(), null), new[] { \"b\", \"a\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"bb\", \"aa\", \"c\" }.OrderBy(x => x.Length).ThenBy(x => x.ToUpperInvariant()), new[] { \"c\", \"aa\", \"bb\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"aa\", \"bb\", \"c\" }.OrderBy(x => x.Length).ThenByDescending(x => x.ToUpperInvariant(), (IComparer<string>)null), new[] { \"c\", \"bb\", \"aa\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(Enumerable.OrderBy(new[] { \"b\", \"a\" }, x => x.ToUpperInvariant(), StringComparer.Ordinal), new[] { \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new[] { \"bb\", \"a\" }.OrderBy(x => (long)x.Length), new[] { \"a\", \"bb\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(Enumerable.ThenBy(Enumerable.OrderByDescending(new[] { \"aa\", \"b\", \"bb\" }, x => x.Length), x => x.Substring(0, 1)), new[] { \"aa\", \"bb\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrArrayOrderingPreservesTypedSelectors(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem());
                session.SaveChanges();
            }

            const string indexName = "PrimitiveArrays/ClrOrdering";
            var compilationError = Record.Exception(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            })));
            Assert.Null(compilationError);
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.True(session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single().Found, expression);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderBy(x => x == null ? null : x.ToUpperInvariant()).ThenBy(x => x, StringComparer.Ordinal), new string[] { null, \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderByDescending(x => x == null ? null : x.ToUpperInvariant()), new string[] { \"b\", \"a\", null })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderBy(x => x == null ? null : (x == \"b\" ? \"B\" : x.ToUpperInvariant())), new string[] { null, \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"bb\", null, \"a\" }.OrderBy(x => x == null ? (int?)null : x.Length), new string[] { null, \"a\", \"bb\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderBy(x => (x == null ? null : x).ToString()), new string[] { null, \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderBy(x => x == null ? null : x.ToUpperInvariant(), StringComparer.Ordinal), new string[] { null, \"a\", \"b\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderByDescending(x => x == null ? null : x.ToUpperInvariant(), (IComparer<string>)null), new string[] { \"b\", \"a\", null })", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrArrayOrderingPreservesConditionalNullKey(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem());
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClrArrayOrderingExecutesInReduce(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem());
                session.Store(new PrimitiveArrayItem());
                session.SaveChanges();
            }

            const string indexName = "PrimitiveArrays/ClrOrderingReduce";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Group = 0, Found = true }" },
                Reduce = "from result in results group result by result.Group into grouped select new { Group = grouped.Key, " +
                         "Found = Enumerable.SequenceEqual(new string[] { \"b\", null, \"a\" }.OrderBy(x => x == null ? null : x.ToUpperInvariant()).ThenBy(x => x, StringComparer.Ordinal), new string[] { null, \"a\", \"b\" }) }",
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.True(session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single().Found);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void StoredArrayEnumerationPreservesValuesInMapAndReduce(Options options, bool reduce, bool generic)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/Enumeration";
            string copy = $"ArrayEnumeration.Copy(item.Values, {generic.ToString().ToLowerInvariant()})";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ArrayEnumerationItems select new { item.Group, Values = " + (reduce ? "item.Values" : copy) + " }" },
                Reduce = reduce
                    ? "from result in results group result by result.Group into grouped let item = grouped.First() select new { Group = grouped.Key, Values = " + copy + " }"
                    : null,
                Fields = { ["Values"] = new IndexFieldOptions { Storage = FieldStorage.Yes } },
                AdditionalSources = new Dictionary<string, string> { ["ArrayEnumeration.cs"] = """
                    using System;
                    using System.Collections;
                    using System.Collections.Generic;

                    public static class ArrayEnumeration
                    {
                        public static object[] Copy(dynamic values, bool generic)
                        {
                            var copied = new List<object>();
                            IEnumerator iterator = generic
                                ? ((IEnumerable<object>)values).GetEnumerator()
                                : ((IEnumerable)values).GetEnumerator();
                            try
                            {
                                while (iterator.MoveNext())
                                {
                                    object value = iterator.Current;
                                    copied.Add(value == null ? "<null>" : value.GetType().Name + ":" + value);
                                }
                            }
                            finally
                            {
                                (iterator as IDisposable)?.Dispose();
                            }
                            return copied.ToArray();
                        }
                    }
                    """ }
            }));
            object[] expected = { 1L, 2.5d, "text", null, new string('x', 1024) };
            using (var session = store.OpenSession())
            {
                session.Store(new ArrayEnumerationItem { Group = "values", Values = expected });
                session.Store(new ArrayEnumerationItem { Group = "empty", Values = Array.Empty<object>() });
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                ArrayEnumerationItem[] results = session.Advanced.RawQuery<ArrayEnumerationItem>($"from index '{indexName}' select Values").ToList().OrderBy(x => x.Values.Length).ToArray();
                Assert.Equal(2, results.Length);
                Assert.Empty(results[0].Values);
                Assert.Equal(new object[]
                {
                    "Int64:1", "LazyNumberValue:2.5", "LazyStringValue:text", "DynamicNullObject:", "LazyStringValue:" + new string('x', 1024)
                }, results[1].Values);
            }
        }

        private sealed class ArrayEnumerationItem
        {
            public string Group { get; set; }
            public object[] Values { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void ContainsComparerPreservesEnumerationAndExceptionBehavior(int scenario)
        {
            bool Run(List<string> trace, bool useDynamicArray)
            {
                var values = new DynamicArray(TraceSequence(trace, "source", 1L, scenario == 2));
                long target = scenario == 0 ? 1L : scenario == 1 ? 9L : 2L;
                var comparer = EqualityComparer<object>.Create((x, y) =>
                {
                    trace.Add($"compare: {x}/{y}");
                    if (scenario == 3)
                        throw new InvalidOperationException("comparer");
                    return Equals(x, y);
                }, _ => 0);
                return useDynamicArray ? values.Contains(target, (IEqualityComparer)comparer) : Enumerable.Contains(values, (object)target, comparer);
            }

            var expectedTrace = new List<string>();
            var actualTrace = new List<string>();
            bool? expected = null;
            bool? actual = null;
            var expectedException = Record.Exception(() => expected = Run(expectedTrace, false));
            var actualException = Record.Exception(() => actual = Run(actualTrace, true));
            Assert.Equal(expected, actual);
            Assert.Equal(expectedException?.GetType(), actualException?.GetType());
            Assert.Equal(expectedException?.Message, actualException?.Message);
            Assert.Equal(expectedTrace, actualTrace);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ContainsComparerPreservesDerivedEnumeration()
        {
            var array = new AlternateEnumerationArray();
            Assert.True(array.Contains(2L, (IEqualityComparer)EqualityComparer<object>.Default));
            Assert.False(array.Contains(1L, (IEqualityComparer)EqualityComparer<object>.Default));
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false)]
        [InlineData(true)]
        public void ContainsPreservesBoxedInt32IdentityAndDerivedEnumeration(bool enumerableOrigin)
        {
            object needle = 42;
            List<string> trace = new List<string>();
            var source = new DisposableEnumerable(new EqualityCallbackValue(other =>
            {
                if (enumerableOrigin)
                {
                    Assert.Same(needle, other);
                    trace.Add("Equals:original");
                    return true;
                }

                Assert.Equal(42L, Assert.IsType<long>(other));
                trace.Add("Equals:Int64");
                return false;
            }), trace);
            DynamicArray values = new AlternateEnumerationArray(source);
            Output.WriteLine("Boxed Int32 Contains server: {0}", typeof(DynamicArray).Assembly.Location);
            bool found = enumerableOrigin
                ? DynamicEnumerable.Contains<object>(values, needle, EqualityComparer<object>.Default)
                : values.Contains(needle);

            Assert.Equal(enumerableOrigin, found);
            Assert.Equal(1, source.EnumeratorCount);
            Assert.Equal(1, source.DisposeCount);
            Assert.Equal(enumerableOrigin
                ? new[] { "GetEnumerator", "MoveNext:true", "Equals:original", "Dispose" }
                : new[] { "GetEnumerator", "MoveNext:true", "Equals:Int64", "MoveNext:false", "Dispose" }, trace);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false)]
        [InlineData(true)]
        public void ContainsPreservesNullNeedleForDerivedCollection(bool enumerableOrigin)
        {
            NullNeedleCollection values = new NullNeedleCollection();
            bool found = enumerableOrigin
                ? DynamicEnumerable.Contains<object>(values, null)
                : values.Contains(null);

            Assert.Equal(1, values.ContainsCalls);
            Assert.Null(values.ReceivedNeedle);
            Assert.True(found);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false)]
        [InlineData(true)]
        public void ConcatPreservesTypedDerivedEnumeration(bool derivedFirst)
        {
            IEnumerable<char> values = new TypedConcatArray();
            IEnumerable<char> result = derivedFirst
                ? DynamicEnumerable.ConcatFromEnumerable<char>(values, new[] { 'c' })
                : DynamicEnumerable.ConcatFromEnumerable<char>(new[] { 'c' }, values);

            Assert.Equal(derivedFirst ? new[] { 'a', 'b', 'c' } : new[] { 'c', 'a', 'b' }, result);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ConcatPreservesTypedArgumentValidation()
        {
            IEnumerable<object> values = new DynamicArray(new object[] { 1L });
            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ConcatFromEnumerable<object>(null, values)).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ConcatFromEnumerable<object>(values, null)).ParamName);
            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ConcatFromEnumerable<object>(null, null)).ParamName);
        }

        private sealed class TypedConcatArray : DynamicArray, IEnumerable<char>
        {
            public TypedConcatArray() : base(new object[] { 1L })
            {
            }

            IEnumerator<char> IEnumerable<char>.GetEnumerator() => ((IEnumerable<char>)new[] { 'a', 'b' }).GetEnumerator();
        }

        private sealed class NullNeedleCollection : DynamicArray, ICollection<object>
        {
            public NullNeedleCollection() : base(new object[] { null })
            {
            }

            public int ContainsCalls { get; private set; }
            public object ReceivedNeedle { get; private set; }

            bool ICollection<object>.Contains(object item)
            {
                ContainsCalls++;
                ReceivedNeedle = item;
                return item == null;
            }

            int ICollection<object>.Count => 1;
            bool ICollection<object>.IsReadOnly => true;
            void ICollection<object>.Add(object item) => throw new NotSupportedException();
            void ICollection<object>.Clear() => throw new NotSupportedException();
            bool ICollection<object>.Remove(object item) => throw new NotSupportedException();
            void ICollection<object>.CopyTo(object[] array, int index) => throw new NotSupportedException();
        }

        private sealed class AlternateEnumerationArray : DynamicArray, IEnumerable<object>
        {
            private readonly IEnumerable<object> _values;

            public AlternateEnumerationArray() : this(new object[] { 2L })
            {
            }

            public AlternateEnumerationArray(IEnumerable<object> values) : base(new object[] { 1L })
            {
                _values = values;
            }

            IEnumerator<object> IEnumerable<object>.GetEnumerator() => _values.GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<object>)this).GetEnumerator();
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public void CharacterSequenceEqualityPreservesLazyStringState(bool cached, bool disposed)
        {
            int[] characters = { 0, 97, 127, 128, 2047, 2048, 55295, 55296, 56319, 56320, 57343, 65535 };
            foreach (int code in characters)
            {
                using var context = Sparrow.Json.JsonOperationContext.ShortTermSingleUse();
                using var reader = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["Value"] = ((char)code).ToString()
                }, "character-equality");
                Assert.True(reader.TryGet("Value", out Sparrow.Json.LazyStringValue text));
                Assert.False(text.HasStringValue);
                if (cached)
                    _ = text.ToString();

                // Keep the backing context alive while exercising the existing disposed-value guard.
                text.IsDisposed = disposed;
                try
                {
                    foreach (char character in new[] { (char)code, 'z' })
                    {
                        bool? expected = null;
                        Exception expectedException = Record.Exception(() => expected = text.Equals(character.ToString()));
                        object[] left = { text };
                        object[] right = { character };
                        Func<bool>[] operations =
                        {
                            () => DynamicEnumerable.SequenceEqual(left, right),
                            () => DynamicEnumerable.SequenceEqual(right, left),
                            () => new DynamicArray(left).SequenceEqual(right)
                        };
                        foreach (Func<bool> operation in operations)
                        {
                            bool? actual = null;
                            Exception actualException = Record.Exception(() => actual = operation());
                            Assert.Equal(expectedException?.GetType(), actualException?.GetType());
                            Assert.Equal(expected, actual);
                            Assert.Equal(cached, text.HasStringValue);
                        }
                    }
                }
                finally
                {
                    text.IsDisposed = false;
                }
            }
        }


        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterSequenceEqualityPreservesUtf8InMapAndReduce(Options options, bool reduce, bool cached)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/CharacterSequence";
            string membership = "Enumerable.SequenceEqual(item.Letters, new[] { (char)item.Code }) && " +
                                      "Enumerable.SequenceEqual(new[] { (char)item.Code }, item.Letters) && " +
                                      "Enumerable.SequenceEqual(item.Letters.ToArray(), new[] { (char)item.Code }) && " +
                                      "Enumerable.SequenceEqual(new[] { (char)item.Code }, item.Letters.ToArray())";
            string preparation = "";
            if (cached)
            {
                preparation = "let letters = (object[])item.Letters.ToArray() let strings = letters.Select(letter => letter.ToString()).ToArray() ";
                membership = "strings.Length == letters.Length && " + membership.Replace("item.Letters", "letters");
            }
            var definition = new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.CharacterSequenceItems " + preparation + "select new { item.Code, item.Letters, Found = " + membership + " }" },
                Fields =
                {
                    ["Code"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
                }
            };
            if (reduce)
                definition.Reduce = "from result in results group result by result.Code into grouped let item = grouped.First() " +
                                    preparation + "select new { item.Code, item.Letters, Found = " + membership + " }";
            store.Maintenance.Send(new PutIndexesOperation(definition));
            int[] characters = { 0, 97, 127, 128, 2047, 2048, 55295, 55296, 56319, 56320, 57343, 65535 };
            using (var session = store.OpenSession())
            {
                foreach (int character in characters)
                    session.Store(new CharacterSequenceItem { Code = character, Letters = new[] { (char)character } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                var results = session.Advanced.RawQuery<CharacterSequenceResult>($"from index '{indexName}' order by Code as long select Code, Found").ToArray();
                Assert.Equal(characters, results.Select(x => x.Code));
                // Preserve LazyStringValue's existing cached/uncached surrogate behavior.
                Assert.All(results, result => Assert.Equal(cached == false || char.IsSurrogate((char)result.Code) == false, result.Found));
            }
        }

        private sealed class CharacterSequenceItem
        {
            public int Code { get; set; }
            public char[] Letters { get; set; }
        }

        private sealed class CharacterSequenceResult
        {
            public int Code { get; set; }
            public bool Found { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("DynamicEnumerable.Min(values)", 1L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Max(values)", 2L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Concat(values, new object[] { 3L }).First()", 1L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new DynamicArray(values).Contains('a') ? 1L : 0L", 0L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new DynamicArray(Enumerable.Where(values, x => false)).Contains('a') ? 1L : 0L", 0L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new DynamicArray(Enumerable.Select(values, x => 'a')).Contains('a') ? 1L : 0L", 1L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Min(EnumerableLifetime.NonGenericCurrent(values))", 1L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("DynamicEnumerable.Max(EnumerableLifetime.NonGenericCurrent(values))", 2L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new DynamicArray(values).Contains(1L, (IEqualityComparer)EqualityComparer<object>.Default) ? 1L : 0L", 1L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new DynamicArray(Enumerable.Where(values, x => false)).Contains('a', (IEqualityComparer)EqualityComparer<char>.Default) ? 1L : 0L", 0L, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("new DynamicArray(Enumerable.Select(values, x => 'a')).Contains('a', (IEqualityComparer)EqualityComparer<char>.Default) ? 1L : 0L", 1L, SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexExecutionDisposesOwnedEnumerators(Options options, string expression, long expected)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/OwnedEnumerators";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems let values = EnumerableLifetime.Track(item.Values) select new { Value = " + expression + ", Disposed = values.Disposed }" },
                Fields =
                {
                    ["Value"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["Disposed"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
                },
                AdditionalSources = new Dictionary<string, string> { ["EnumerableLifetime.cs"] = """
                    using System;
                    using System.Collections;
                    using System.Collections.Generic;

                    public static class EnumerableLifetime
                    {
                        public static dynamic Track(dynamic values) => new OwnedEnumerable((IEnumerable)values);
                        public static dynamic NonGenericCurrent(object source)
                        {
                            var values = (OwnedEnumerable)source;
                            values.ReadNonGenericCurrent = true;
                            return values;
                        }

                        public sealed class OwnedEnumerable : IEnumerable<object>
                        {
                            private readonly IEnumerable _values;
                            public bool Disposed;
                            public bool ReadNonGenericCurrent;
                            public OwnedEnumerable(IEnumerable values) { _values = values; }
                            public IEnumerator<object> GetEnumerator() => new OwnedEnumerator(this, _values.GetEnumerator());
                            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

                            private sealed class OwnedEnumerator : IEnumerator<object>
                            {
                                private readonly OwnedEnumerable _owner;
                                private readonly IEnumerator _inner;
                                public OwnedEnumerator(OwnedEnumerable owner, IEnumerator inner) { _owner = owner; _inner = inner; }
                                public object Current => _owner.ReadNonGenericCurrent ? throw new InvalidOperationException("Generic Current must not be read") : _inner.Current;
                                object IEnumerator.Current => _inner.Current;
                                public bool MoveNext() => _inner.MoveNext();
                                public void Reset() { throw new NotSupportedException(); }
                                public void Dispose()
                                {
                                    _owner.Disposed = true;
                                    (_inner as IDisposable)?.Dispose();
                                }
                            }
                        }
                    }
                    """ }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Values = new[] { 1L, 2L } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<OwnedEnumerationResult>($"from index '{indexName}' select Value, Disposed").Single();
                Assert.Equal(expected, result.Value);
                Assert.True(result.Disposed, expression);
            }
        }

        private sealed class OwnedEnumerationResult
        {
            public long Value { get; set; }
            public bool Disposed { get; set; }
        }

        private sealed class CharSequenceEqualIndex : AbstractIndexCreationTask<PrimitiveArrayItem, ClrContainsResult>
        {
            public CharSequenceEqualIndex()
            {
                Map = items => from item in items
                               select new ClrContainsResult { Found = item.Letters.SequenceEqual(new[] { 'a', 'b' }) };
                Store(x => x.Found, FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void SequenceEqualPreservesEnumerationAndExceptionBehavior(int scenario)
        {
            var expectedTrace = new List<string>();
            var actualTrace = new List<string>();
            bool? expected = null;
            bool? actual = null;
            var expectedException = Record.Exception(() => expected = Enumerable.SequenceEqual(
                new DynamicArray(TraceSequence(expectedTrace, "first", 1L, scenario == 2)),
                TraceSequence(expectedTrace, "second", scenario == 1 ? 3L : 1L, scenario == 3)));
            var actualException = Record.Exception(() => actual = new DynamicArray(
                    TraceSequence(actualTrace, "first", 1L, scenario == 2))
                .SequenceEqual(TraceSequence(actualTrace, "second", scenario == 1 ? 3L : 1L, scenario == 3)));

            Assert.Equal(expected, actual);
            Assert.Equal(expectedException?.GetType(), actualException?.GetType());
            Assert.Equal(expectedException?.Message, actualException?.Message);
            Assert.Equal(expectedTrace, actualTrace);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void ClrSequenceEqualPreservesEnumerationAndExceptionBehavior(int scenario)
        {
            var expectedTrace = new List<string>();
            var actualTrace = new List<string>();
            bool? expected = null;
            bool? actual = null;
            var expectedException = Record.Exception(() => expected = Enumerable.SequenceEqual(
                TraceSequence(expectedTrace, "first", 1L, scenario == 2),
                TraceSequence(expectedTrace, "second", scenario == 1 ? 3L : 1L, scenario == 3)));
            var actualException = Record.Exception(() => actual = DynamicEnumerable.SequenceEqual(
                TraceSequence(actualTrace, "first", 1L, scenario == 2),
                TraceSequence(actualTrace, "second", scenario == 1 ? 3L : 1L, scenario == 3)));

            Assert.Equal(expected, actual);
            Assert.Equal(expectedException?.GetType(), actualException?.GetType());
            Assert.Equal(expectedException?.Message, actualException?.Message);
            Assert.Equal(expectedTrace, actualTrace);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public void ContainsPreservesCollectionMutationBehavior(bool nullComparer, bool nonGeneric)
        {
            var expectedValues = new List<object>();
            var actualValues = new List<object>();
            expectedValues.Add(new MutatingEqualityValue(() => expectedValues.Add('b')));
            actualValues.Add(new MutatingEqualityValue(() => actualValues.Add('b')));
            bool? expected = null;
            bool? actual = null;
            var expectedException = Record.Exception(() => expected = nullComparer
                ? Enumerable.Contains(expectedValues, (object)'b', comparer: null)
                : Enumerable.Contains(expectedValues, (object)'b'));
            var actualException = Record.Exception(() =>
            {
                if (nonGeneric)
                    actual = nullComparer
                        ? DynamicEnumerable.Contains((object)actualValues, 'b', (IEqualityComparer)null)
                        : DynamicEnumerable.Contains((object)actualValues, 'b');
                else
                    actual = nullComparer
                        ? DynamicEnumerable.Contains(actualValues, (object)'b', comparer: null)
                        : DynamicEnumerable.Contains(actualValues, (object)'b');
            });

            Assert.Equal(expected, actual);
            Assert.Equal(expectedException?.GetType(), actualException?.GetType());
            Assert.Equal(expectedException?.Message, actualException?.Message);
            Assert.Equal(expectedValues.Count, actualValues.Count);
        }

        private sealed class MutatingEqualityValue
        {
            private readonly Action _onEquals;

            public MutatingEqualityValue(Action onEquals)
            {
                _onEquals = onEquals;
            }

            public override bool Equals(object obj)
            {
                _onEquals();
                return false;
            }

            public override int GetHashCode() => 0;
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ContainsPreservesCollectionComparerAndTypedNullArguments()
        {
            var values = new HashSet<object>(EqualityComparer<object>.Create((x, y) => true, x => 0)) { 'a' };
            Assert.True(Enumerable.Contains(values, (object)'b'));
            Assert.True(DynamicEnumerable.Contains(values, (object)'b'));
            Assert.True(DynamicEnumerable.Contains((object)values, 'b'));
            Assert.False(DynamicEnumerable.Contains(values, (object)'b', comparer: null));
            Assert.False(DynamicEnumerable.Contains((object)values, 'b', (IEqualityComparer)null));

            Assert.Equal("source", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.Contains<object>(null, 'b')).ParamName);
            Assert.Equal("source", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.Contains<object>(null, 'b', null)).ParamName);
            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.SequenceEqual<object>(null, Array.Empty<object>())).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.SequenceEqual(Array.Empty<object>(), (IEnumerable<object>)null)).ParamName);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false)]
        [InlineData(true)]
        public void ContainsPreservesCovariantArraysAndCustomEquality(bool nonGeneric)
        {
            bool Contains(object[] values, object value) => nonGeneric
                ? DynamicEnumerable.Contains((object)values, value)
                : DynamicEnumerable.Contains(values, value);

            object[] strings = new string[] { "a", null };
            Assert.True(Contains(strings, null));
            Assert.False(Contains(strings, 'a'));
            Assert.False(Contains(new string[] { "a" }, null));

            int calls = 0;
            object[] customValues = new EqualityCallbackValue[]
            {
                new(value =>
                {
                    calls++;
                    Assert.Equal((object)'b', value);
                    return true;
                })
            };
            Assert.False(Contains(customValues, null));
            Assert.Equal(0, calls);
            Assert.True(Contains(customValues, 'b'));
            Assert.Equal(1, calls);

            object[] throwingValues = new EqualityCallbackValue[] { new(_ => throw new InvalidOperationException("equality")) };
            Assert.Equal("equality", Assert.Throws<InvalidOperationException>(() => Contains(throwingValues, 'b')).Message);
        }

        private sealed class EqualityCallbackValue
        {
            private readonly Func<object, bool> _equals;

            public EqualityCallbackValue(Func<object, bool> equals)
            {
                _equals = equals;
            }

            public override bool Equals(object obj) => _equals(obj);

            public override int GetHashCode() => 0;
        }

        private static IEnumerable<object> TraceSequence(List<string> trace, string name, long first, bool throwAfterFirst)
        {
            try
            {
                trace.Add(name + ": first");
                yield return first;
                if (throwAfterFirst)
                    throw new InvalidOperationException(name);

                trace.Add(name + ": second");
                yield return 2L;
                trace.Add(name + ": end");
            }
            finally
            {
                trace.Add(name + ": disposed");
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Concat((IEnumerable<object>)item.Tags, new object[] { \"2024-01-01T00:00:00.0000000Z\" }).Last() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new object[] { \"2024-01-01T00:00:00.0000000Z\" }, (IEnumerable<object>)item.Tags).First() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(item.Tags, new[] { \"suffix\" }).Where(x => x == \"suffix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new[] { \"prefix\" }, item.Tags).Where(x => x == \"prefix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(item.Tags, item.Tags).Where(x => x == \"a\").Count() == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void EnumerableConcatWrapsOnlyDynamicallyBoundStoredArrays(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Concat(new DynamicArray(new object[] { 1L }), new object[] { \"2024-01-01T00:00:00.0000000Z\" }).Last() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new object[] { \"2024-01-01T00:00:00.0000000Z\" }, new DynamicArray(new object[] { 1L })).First() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("((IEnumerable<object>)Enumerable.Concat(item.Tags, new object[] { \"2024-01-01T00:00:00.0000000Z\" })).Last() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("((IEnumerable<object>)Enumerable.Concat(new object[] { \"2024-01-01T00:00:00.0000000Z\" }, item.Tags)).First() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("((IEnumerable<object>)Enumerable.Concat(item.MissingTags, new object[] { \"2024-01-01T00:00:00.0000000Z\" })).Last() is string", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("false ? Enumerable.Concat(null, new object[] { \"x\" }).Count() > 0 : true", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("false ? Enumerable.Concat(new object[] { \"x\" }, null).Count() > 0 : true", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("false ? Enumerable.Concat(null, new DynamicArray(new object[] { \"x\" })).Count() > 0 : true", SearchEngineMode = RavenSearchEngineMode.All)]
        public void EnumerableConcatPreservesReleasedForms(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Concat(item.MissingTags, new[] { \"suffix\" }).Where(x => x == \"suffix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(new[] { \"prefix\" }, item.MissingTags).Where(x => x == \"prefix\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(item.MissingTags, item.MissingTags).Where(x => x == \"a\").Count() == 0", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(item.MissingTags, item.Tags).Where(x => x == \"a\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Concat(item.Tags, item.MissingTags).Where(x => x == \"a\").Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        public void EnumerableConcatTreatsMissingValueAsEmptyDynamicArray(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Count(source: Enumerable.Concat(item.Tags, new ConcatCountOnlyCollection(2))) == 4", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Concat(new ConcatCountOnlyCollection(1), Enumerable.Concat(item.MissingTags, new ConcatCountOnlyCollection(2)))) == 3", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Concat(Enumerable.Concat(item.Tags, new ConcatCountOnlyCollection(1)), item.Tags)) == 5", SearchEngineMode = RavenSearchEngineMode.All)]
        public void EnumerableCountOfConcatDoesNotEnumerateCountableOperands(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ConcatCount";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } },
                // Count must use this operand's Count property; enumerating it fails the index.
                // Additional sources are rewritten: public methods need block bodies, and CopyTo is explicit so its signature is not made dynamic.
                AdditionalSources = new Dictionary<string, string> { ["ConcatCountOnlyCollection.cs"] = """
                    using System;
                    using System.Collections;
                    using System.Collections.Generic;
                    public sealed class ConcatCountOnlyCollection : ICollection<object>
                    {
                        private readonly int _count;
                        public ConcatCountOnlyCollection(int count)
                        {
                            _count = count;
                        }
                        public int Count => _count;
                        public bool IsReadOnly => true;
                        public IEnumerator<object> GetEnumerator() { throw new InvalidOperationException("The countable Concat operand was enumerated."); }
                        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
                        public bool Contains(object item) { throw new NotSupportedException(); }
                        void ICollection<object>.CopyTo(object[] array, int arrayIndex) { throw new NotSupportedException(); }
                        public void Add(object item) { throw new NotSupportedException(); }
                        public bool Remove(object item) { throw new NotSupportedException(); }
                        public void Clear() { throw new NotSupportedException(); }
                    }
                    """ }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single();
                Assert.True(result.Found, expression);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.ToArray(source: Enumerable.Concat(item.Tags, new ConcatCopyOnlyCollection(2))).Length == 4", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.ToArray(Enumerable.Concat(new ConcatCopyOnlyCollection(1), item.Tags))[0] == \"copied\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.ToArray(Enumerable.Concat(Enumerable.Concat(item.Tags, new ConcatCopyOnlyCollection(1)), item.Tags)).Length == 5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.ToList(Enumerable.Concat(item.Tags, new ConcatCopyOnlyCollection(2)))[3] == \"copied\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.ToList(Enumerable.Concat(item.MissingTags, new ConcatCopyOnlyCollection(2))).Count == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void EnumerableToArrayAndToListOfConcatDoNotEnumerateCountableOperands(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ConcatToCollection";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } },
                // As for Count, ToArray and ToList must copy this operand through ICollection<T>.CopyTo; enumerating it fails the index.
                // Additional sources are rewritten: public methods need block bodies, and CopyTo is explicit so its signature is not made dynamic.
                AdditionalSources = new Dictionary<string, string> { ["ConcatCopyOnlyCollection.cs"] = """
                    using System;
                    using System.Collections;
                    using System.Collections.Generic;
                    public sealed class ConcatCopyOnlyCollection : ICollection<object>
                    {
                        private readonly int _count;
                        public ConcatCopyOnlyCollection(int count)
                        {
                            _count = count;
                        }
                        public int Count => _count;
                        public bool IsReadOnly => true;
                        public IEnumerator<object> GetEnumerator() { throw new InvalidOperationException("The countable Concat operand was enumerated."); }
                        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
                        public bool Contains(object item) { throw new NotSupportedException(); }
                        void ICollection<object>.CopyTo(object[] array, int arrayIndex)
                        {
                            for (int i = 0; i < _count; i++)
                                array[arrayIndex + i] = "copied";
                        }
                        public void Add(object item) { throw new NotSupportedException(); }
                        public bool Remove(object item) { throw new NotSupportedException(); }
                        public void Clear() { throw new NotSupportedException(); }
                    }
                    """ }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single();
                Assert.True(result.Found, expression);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("(object)Enumerable.ToArray(new string[] { \"a\" }) is string[]", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(object)Enumerable.ToList(new string[] { \"a\" }) is List<string>", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(object)Enumerable.ToArray(item.Tags[0]) is char[]", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.ToList(item.Tags).Count == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.ToArray(Enumerable.Concat(item.Tags, new[] { \"suffix\" }))[2] == \"suffix\"", SearchEngineMode = RavenSearchEngineMode.All)]
        public void EnumerableToArrayAndToListKeepBclElementAndResultTypes(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a", "b" } });
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ConcatFromEnumerableKeepsBclElementsAndArgumentErrors()
        {
            DynamicArray values = new DynamicArray(new object[] { 1L });
            IEnumerable<object> typed = values;
            object[] text = { "2024-01-01T00:00:00.0000000Z" };

            // Declared stored and missing values bind like dynamic ones, but the other operand keeps its CLR value.
            IEnumerable<object> wrapped = DynamicEnumerable.ConcatFromEnumerable(values, text);
            Assert.IsAssignableFrom<DynamicArray>(wrapped);
            Assert.IsType<string>(wrapped.Last());
            wrapped = DynamicEnumerable.ConcatFromEnumerable(text, values);
            Assert.IsAssignableFrom<DynamicArray>(wrapped);
            Assert.IsType<string>(wrapped.First());
            Assert.IsAssignableFrom<DynamicArray>(DynamicEnumerable.ConcatFromEnumerable(values, values));
            wrapped = DynamicEnumerable.ConcatFromEnumerable(DynamicNullObject.Null, text);
            Assert.IsAssignableFrom<DynamicArray>(wrapped);
            Assert.IsType<string>(Assert.Single(wrapped));
            // Index output converters enumerate through IEnumerable; it must yield the same CLR value as IEnumerable<object>.
            IEnumerator untyped = ((IEnumerable)wrapped).GetEnumerator();
            Assert.True(untyped.MoveNext());
            Assert.IsType<string>(untyped.Current);
            Assert.False(untyped.MoveNext());
            IEnumerable<object> clr = DynamicEnumerable.ConcatFromEnumerable(typed, text);
            Assert.IsNotType<DynamicArray>(clr);
            Assert.IsType<string>(clr.Last());

            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ConcatFromEnumerable((DynamicArray)null, text)).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ConcatFromEnumerable(text, (DynamicArray)null)).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ConcatFromEnumerable(values, (DynamicArray)null)).ParamName);
            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ConcatFromEnumerable((DynamicArray)null, (DynamicArray)null)).ParamName);

            // A literal null beside a dynamic operand keeps the eager BCL argument error instead of an ambiguous binding.
            dynamic stored = values;
            dynamic missing = DynamicNullObject.Null;
            dynamic raw = text;
            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => { DynamicEnumerable.ConcatFromEnumerable(null, stored); }).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => { DynamicEnumerable.ConcatFromEnumerable(stored, null); }).ParamName);
            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => { DynamicEnumerable.ConcatFromEnumerable(null, missing); }).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => { DynamicEnumerable.ConcatFromEnumerable(missing, null); }).ParamName);
            Assert.Equal("first", Assert.Throws<ArgumentNullException>(() => { DynamicEnumerable.ConcatFromEnumerable(null, raw); }).ParamName);
            Assert.Equal("second", Assert.Throws<ArgumentNullException>(() => { DynamicEnumerable.ConcatFromEnumerable(raw, null); }).ParamName);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void NestedConcatAndCountKeepElementsDisposalAndErrors()
        {
            var trace = new List<string>();
            // A Concat result passed back into Concat keeps its chaining, element order and operand disposal.
            var inner = (DynamicArray)DynamicEnumerable.ConcatFromEnumerable(DynamicNullObject.Null, TraceSequence(trace, "first", 1L, false));
            IEnumerable<object> nested = DynamicEnumerable.ConcatFromEnumerable(inner, TraceSequence(trace, "second", 3L, false));
            Assert.IsAssignableFrom<DynamicArray>(nested);
            Assert.Equal(new object[] { 1L, 2L, 3L, 2L }, nested.ToArray());
            nested = DynamicEnumerable.ConcatFromEnumerable(TraceSequence(trace, "third", 5L, false), inner);
            Assert.IsAssignableFrom<DynamicArray>(nested);
            Assert.Equal(new object[] { 5L, 2L, 1L, 2L }, nested.ToArray());
            Assert.Equal(new[]
            {
                "first: first", "first: second", "first: end", "first: disposed",
                "second: first", "second: second", "second: end", "second: disposed",
                "third: first", "third: second", "third: end", "third: disposed",
                "first: first", "first: second", "first: end", "first: disposed"
            }, trace);

            Assert.Equal("source", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.CountFromEnumerable(null)).ParamName);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ToArrayAndToListFromEnumerableKeepBclResultsAndArgumentErrors()
        {
            IEnumerable<object> concatenated = DynamicEnumerable.ConcatFromEnumerable(new DynamicArray(new object[] { 1L }), new object[] { "2024-01-01T00:00:00.0000000Z" });
            // A Concat result converts to the elements of its own enumeration, in the same order.
            Assert.Equal(concatenated.ToArray(), DynamicEnumerable.ToArrayFromEnumerable(concatenated));
            Assert.Equal(concatenated.ToList(), DynamicEnumerable.ToListFromEnumerable(concatenated));

            // Any other source reaches the BCL call with the same inferred element type, including through dynamic binding.
            object result = DynamicEnumerable.ToArrayFromEnumerable(new[] { "a" });
            Assert.IsType<string[]>(result);
            result = DynamicEnumerable.ToListFromEnumerable(new[] { "a" });
            Assert.IsType<List<string>>(result);
            dynamic text = "ab";
            result = DynamicEnumerable.ToArrayFromEnumerable(text);
            Assert.Equal(new[] { 'a', 'b' }, Assert.IsType<char[]>(result));
            dynamic stored = new DynamicArray(new object[] { 1L });
            result = DynamicEnumerable.ToListFromEnumerable(stored);
            Assert.IsType<List<object>>(result);

            Assert.Equal("source", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ToArrayFromEnumerable((IEnumerable<object>)null)).ParamName);
            Assert.Equal("source", Assert.Throws<ArgumentNullException>(() => DynamicEnumerable.ToListFromEnumerable((IEnumerable<char>)null)).ParamName);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(false)]
        [InlineData(true)]
        public void ContainsMatchesNullNeedleWithoutCallingElementEquality(bool enumerableOrigin)
        {
            bool Contains(DynamicArray values) => enumerableOrigin
                ? DynamicEnumerable.Contains<object>(values, null)
                : values.Contains(null);

            int calls = 0;
            Assert.False(Contains(new DynamicArray(new object[] { new EqualityCallbackValue(_ => { calls++; return true; }) })));
            Assert.Equal(0, calls);
            Assert.True(Contains(new AlternateEnumerationArray(new object[] { 1L, null })));
            Assert.False(Contains(new AlternateEnumerationArray(new object[] { 1L })));
            Assert.True(Contains(new DynamicArray(new object[] { 1L, null })));
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void DisposingDynamicArrayEnumeratorDisposesWrappedEnumerator()
        {
            var source = new DisposableEnumerable();
            var array = new DynamicArray(source);

            using (var enumerator = array.GetEnumerator())
                Assert.True(enumerator.MoveNext());

            Assert.Equal(1, source.EnumeratorCount);
            Assert.Equal(1, source.DisposeCount);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MinAndMaxDisposeTheirEnumerators()
        {
            var minSource = new DisposableEnumerable();
            Assert.Equal(1L, DynamicEnumerable.Min(minSource));
            Assert.Equal(1, minSource.EnumeratorCount);
            Assert.Equal(1, minSource.DisposeCount);

            var maxSource = new DisposableEnumerable();
            Assert.Equal(1L, DynamicEnumerable.Max(maxSource.Cast<object>()));
            Assert.Equal(1, maxSource.EnumeratorCount);
            Assert.Equal(1, maxSource.DisposeCount);
        }

        private sealed class DisposableEnumerable : IEnumerable<object>
        {
            private readonly object _value;
            private readonly List<string> _trace;

            public DisposableEnumerable(object value = null, List<string> trace = null)
            {
                _value = value ?? 1L;
                _trace = trace;
            }

            public int EnumeratorCount { get; private set; }

            public int DisposeCount { get; private set; }

            public IEnumerator<object> GetEnumerator()
            {
                EnumeratorCount++;
                _trace?.Add("GetEnumerator");
                return new DisposableEnumerator(this);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private sealed class DisposableEnumerator : IEnumerator<object>
            {
                private readonly DisposableEnumerable _owner;
                private bool _moved;

                public DisposableEnumerator(DisposableEnumerable owner)
                {
                    _owner = owner;
                }

                public bool MoveNext()
                {
                    _owner._trace?.Add(_moved ? "MoveNext:false" : "MoveNext:true");
                    if (_moved)
                        return false;

                    _moved = true;
                    Current = _owner._value;
                    return true;
                }

                public void Reset()
                {
                    _moved = false;
                }

                public object Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _owner.DisposeCount++;
                    _owner._trace?.Add("Dispose");
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrderByWithComparerCanBeFollowedByAdditionalOrderingLevels(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new OrderByWithComparerIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                var map = Assert.Single(definition.Maps);
                Assert.Contains(".OrderBy(", map);
                Assert.Contains(".ThenByDescending(", map);
                Assert.Contains(".ThenBy(", map);
                Assert.DoesNotContain("Enumerable.OrderBy", map);
                Assert.Contains("StringComparer.OrdinalIgnoreCase", map);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem { Tags = new[] { "a", "A" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<OrderByWithComparerIndex.Result>(
                            $"from index '{index.IndexName}' select First, NullComparerFirst, NullDescendingComparerFirst")
                        .Single();
                    Assert.Equal("A", result.First);
                    Assert.Equal("a", result.NullComparerFirst);
                    Assert.Equal("a", result.NullDescendingComparerFirst);
                }
            }
        }

        public class OrderByWithComparerIndex : AbstractIndexCreationTask<PrimitiveArrayItem, OrderByWithComparerIndex.Result>
        {
            public OrderByWithComparerIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   First = item.Tags
                                       .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                                       .ThenByDescending(x => x.Length)
                                       .ThenBy(x => x.ToString() == "A" ? 0 : 1)
                                       .First(),
                                   NullComparerFirst = item.Tags
                                       .OrderBy(x => x.Length)
                                       .ThenBy(x => x == "a" ? 0 : 1, null)
                                       .First(),
                                   NullDescendingComparerFirst = item.Tags
                                       .OrderBy(x => x.Length)
                                       .ThenByDescending(x => x == "a" ? 1 : 0, null)
                                       .First()
                               };

                Store(x => x.First, FieldStorage.Yes);
                Store(x => x.NullComparerFirst, FieldStorage.Yes);
                Store(x => x.NullDescendingComparerFirst, FieldStorage.Yes);
            }

            public class Result
            {
                public string First { get; set; }

                public string NullComparerFirst { get; set; }

                public string NullDescendingComparerFirst { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void PrimitiveArrayLinqOperationsRunWithReleasedDefinitionSpelling(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new PrimitiveArrayLinqOperationsIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                var map = Assert.Single(definition.Maps);
                foreach (var method in new[] { "Contains", "SequenceEqual" })
                {
                    Assert.Contains($"Enumerable.{method}", map);
                    Assert.DoesNotContain($"DynamicEnumerable.{method}", map);
                }

                Assert.Contains("DynamicEnumerable.Concat", map);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem
                    {
                        Values = new[] { 1L, 2L, 3L },
                        Tags = new[] { "a", "b" }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var count = session.Advanced.RawQuery<dynamic>(
                            $"from index '{index.IndexName}' " +
                            "where ContainsTwo = true and ContainsCaseInsensitive = true and ExactSequence = true " +
                            "and ConcatCount = 4")
                        .Count();
                    Assert.Equal(1, count);
                }
            }
        }

        public class PrimitiveArrayLinqOperationsIndex : AbstractIndexCreationTask<PrimitiveArrayItem, PrimitiveArrayLinqOperationsIndex.Result>
        {
            public PrimitiveArrayLinqOperationsIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   ContainsTwo = item.Values.Contains(2L),
                                   ContainsCaseInsensitive = item.Tags.Contains("A", StringComparer.OrdinalIgnoreCase),
                                   ExactSequence = item.Values.SequenceEqual(new[] { 1L, 2L, 3L }),
                                   ConcatCount = item.Values.Concat(new[] { 4L }).Count()
                               };
            }

            public class Result
            {
                public bool ContainsTwo { get; set; }

                public bool ContainsCaseInsensitive { get; set; }

                public bool ExactSequence { get; set; }

                public int ConcatCount { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualOnLongArrayUsesUnchangedDefinition(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new SequenceEqualOnLongArrayIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                var map = Assert.Single(definition.Maps);
                Assert.Contains("Enumerable.SequenceEqual", map);
                Assert.DoesNotContain("DynamicEnumerable.SequenceEqual", map);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem { Values = new[] { 1L, 2L, 3L } });
                    session.Store(new PrimitiveArrayItem { Values = new[] { 1L, 2L } });
                    session.Store(new PrimitiveArrayItem { Values = new[] { 2L, 3L, 4L } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<SequenceEqualOnLongArrayIndex.Result, SequenceEqualOnLongArrayIndex>()
                        .Where(x => x.IsExact)
                        .ToList();
                    Assert.Single(results);
                }
            }
        }

        public class SequenceEqualOnLongArrayIndex : AbstractIndexCreationTask<PrimitiveArrayItem, SequenceEqualOnLongArrayIndex.Result>
        {
            public SequenceEqualOnLongArrayIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   IsExact = item.Values.SequenceEqual(new[] { 1L, 2L, 3L })
                               };
            }

            public class Result
            {
                public bool IsExact { get; set; }
            }
        }

        public class PrimitiveArrayItem
        {
            public string Group { get; set; }

            public long[] Values { get; set; }

            public char[] Letters { get; set; }

            public string[] Tags { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualOnLongArrayWorksInReduce(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new SequenceEqualOnLongArrayMapReduceIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                Assert.Contains("Enumerable.SequenceEqual", definition.Reduce);
                Assert.DoesNotContain("DynamicEnumerable.SequenceEqual", definition.Reduce);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem
                    {
                        Group = "expected",
                        Values = new[] { 1L, 2L, 3L }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<SequenceEqualOnLongArrayMapReduceIndex.Result, SequenceEqualOnLongArrayMapReduceIndex>()
                        .Where(x => x.IsExact)
                        .ToList();
                    Assert.Single(results);
                }
            }
        }

        public class SequenceEqualOnLongArrayMapReduceIndex : AbstractIndexCreationTask<PrimitiveArrayItem, SequenceEqualOnLongArrayMapReduceIndex.Result>
        {
            public SequenceEqualOnLongArrayMapReduceIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   Group = item.Group,
                                   Values = item.Values,
                                   IsExact = false
                               };

                Reduce = results => from result in results
                                    group result by result.Group
                                    into grouped
                                    let values = grouped.First().Values
                                    select new Result
                                    {
                                        Group = grouped.Key,
                                        Values = values,
                                        IsExact = values.SequenceEqual(new[] { 1L, 2L, 3L })
                                    };
            }

            public class Result
            {
                public string Group { get; set; }

                public long[] Values { get; set; }

                public bool IsExact { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOrDefaultOnWhere(Options options)
        {
            IndexWithDynamicSource<FirstOrDefaultOnWhere_Index>(options);
        }

        public class FirstOrDefaultOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOrDefaultOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).FirstOrDefault()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOrDefaultOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<FirstOrDefaultOnWhereWithPredicate_Index>(options);
        }
        public class FirstOrDefaultOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOrDefaultOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).FirstOrDefault(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOnWhere(Options options)
        {
            IndexWithDynamicSource<FirstOnWhere_Index>(options);
        }
        public class FirstOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).First()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<FirstOnWhereWithPredicate_Index>(options);
        }
        public class FirstOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).First(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOrDefaultOnWhere(Options options)
        {
            IndexWithDynamicSource<SingleOrDefaultOnWhere_Index>(options);
        }
        public class SingleOrDefaultOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOrDefaultOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).SingleOrDefault()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOrDefaultOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<SingleOrDefaultOnWhereWithPredicate_Index>(options);
        }
        public class SingleOrDefaultOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOrDefaultOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).SingleOrDefault(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOnWhere(Options options)
        {
            IndexWithDynamicSource<SingleOnWhere_Index>(options);
        }
        public class SingleOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Single()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<SingleOnWhereWithPredicate_Index>(options);
        }
        public class SingleOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Single(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOrDefaultOnWhere(Options options)
        {
            IndexWithDynamicSource<LastOrDefaultOnWhere_Index>(options);
        }
        public class LastOrDefaultOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public LastOrDefaultOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).LastOrDefault()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOrDefaultOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<LastOrDefaultOnWhereWithPredicate_Index>(options);
        }
        public class LastOrDefaultOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public LastOrDefaultOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).LastOrDefault(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOnWhere(Options options)
        {
            IndexWithDynamicSource<LastOnWhere_Index>(options);
        }
        public class LastOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public LastOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Last()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }


        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<LastOnWhereWithPredicate_Index>(options);
        }
        public class LastOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public LastOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Last(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        public void IndexWithDynamicSource<T>(Options options) where T : AbstractIndexCreationTask<Item>, new()
        {
            var index = new T();
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "Items/1", Nodes = new Dictionary<string, string> { { "Items/1", "notsample1" }, { "Items/2", "notsample2" } } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);
                var errors = Indexes.WaitForIndexingErrors(store, new[] { index.IndexName }, errorsShouldExists: false)?
                    .SelectMany(e => e.Errors)
                    .Select(e => e.Error)
                    .ToArray();
                if (errors is not null)
                {
                    var errorsString = string.Join("\n", errors);
                    Assert.DoesNotContain("Failed to execute mapping function", errorsString);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Result, T>().Select(x => x.Path).ToList();
                    switch (typeof(T))
                    {
                        case var i1 when i1 == typeof(FirstOnWhere_Index):
                        case var i2 when i2 == typeof(FirstOrDefaultOnWhere_Index):
                        case var i3 when i3 == typeof(LastOnWhere_Index):
                        case var i4 when i4 == typeof(LastOrDefaultOnWhere_Index):
                        case var i5 when i5 == typeof(SingleOnWhere_Index):
                        case var i6 when i6 == typeof(SingleOrDefaultOnWhere_Index):
                            Assert.Equal(1, query.Count);
                            Assert.Equal(query[0], "notsample1");
                            break;

                        case var i7 when i7 == typeof(LastOnWhereWithPredicate_Index):
                        case var i8 when i8 == typeof(LastOrDefaultOnWhereWithPredicate_Index):
                        case var i9 when i9 == typeof(FirstOnWhereWithPredicate_Index):
                        case var i10 when i10 == typeof(FirstOrDefaultOnWhereWithPredicate_Index):
                        case var i11 when i11 == typeof(SingleOnWhereWithPredicate_Index):
                        case var i12 when i12 == typeof(SingleOrDefaultOnWhereWithPredicate_Index):
                            Assert.Equal(0, query.Count);
                            break;
                    }
                }
            }
        }
        public class Result
        {
            public string Path { get; set; }
        }

        public class Item
        {
            public string Id { get; set; }
            public Dictionary<string, string> Nodes { get; set; }
        }
    }
}
