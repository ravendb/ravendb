using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
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

        private void AssertPrimitiveArrayExpression(Options options, string expression, PrimitiveArrayItem item, Dictionary<string, string> additionalSources = null, string lets = null)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/Expression";
            var definition = new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems " + (string.IsNullOrEmpty(lets) ? "" : lets + " ") + "select new { Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            };
            if (additionalSources != null)
                definition.AdditionalSources = additionalSources;
            store.Maintenance.Send(new PutIndexesOperation(definition));
            using (var session = store.OpenSession())
            {
                session.Store(item);
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), expression + Environment.NewLine + string.Join(Environment.NewLine, errors.Select(x => x.Error)));
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
        // Strings and value sequences are counted by their own element type.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(tag) > 3) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Where(item.Tags, tag => Enumerable.Count(tag) > 3)) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(\"abc\") == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(new[] { 1, 2 }) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Range(0, 2)) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Object sequences, documents and object-typed values are counted through IEnumerable<object>.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(item.Tags) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(item.MissingTags) == 0) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(item) > 0) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count((object)Enumerable.Concat(item.Tags, new object[] { tag })) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(ExplicitSequence.Create()) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A dynamic object with an IEnumerable<object> view is counted through it, without its custom conversion.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(ConvertibleSequence.Create()) == 3 && ConvertibleSequence.TryConvertCalls == 0) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Dictionaries are counted through their object view when dynamic and through their pair view when typed.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count((dynamic)((IEnumerable<object>)item.Tags).ToDictionary(t => t, t => t)) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(((IEnumerable<object>)item.Tags).ToDictionary(t => t, t => t)) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A derived array with an additional typed view is counted through its object view.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(new TypedSequenceValues(new object[] { 1, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The source is evaluated once, also when it is converted.
        [RavenData("Enumerable.Count(item.Tags, tag => CountProbe.Reset() && Enumerable.Count(CountProbe.Next()) == 3 && CountProbe.Calls == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => CountProbe.Reset() && Enumerable.Count(CountProbe.NextExplicit()) == 2 && CountProbe.Calls == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Null sources fail as in Enumerable.Count; an implicit-only conversion is not used by the explicit conversion.
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ParamName((Func<bool>)(() => Enumerable.Count((IEnumerable<int>)null) == 0)) == \"source\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ParamName((Func<bool>)(() => Enumerable.Count(null) == 0)) == \"source\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Count(ImplicitSequence.Create()) == 2)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void CountInsideRewrittenCallsKeepsSourceElementType(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CharacterCountInsideCountPredicateExecutes(Options options, bool filtered)
        {
            using var store = GetDocumentStore(options);
            var index = new CharacterCountPredicateIndex(filtered);
            store.ExecuteIndex(index);
            Assert.Contains("Enumerable.Count(tag)", Assert.Single(index.CreateIndexDefinition().Maps));

            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } });
                session.Store(new PrimitiveArrayItem { Tags = new[] { "abcd", "abcde" } });
                session.Store(new PrimitiveArrayItem { Tags = new string[0] });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName })).Single().Errors;
            Assert.False(errors.Any(), string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var counts = session.Advanced.RawQuery<PrimitiveArrayCountPredicateIndex.Result>($"from index '{index.IndexName}' select Count").ToList();
                Assert.Equal(new[] { 0, 1, 2 }, counts.Select(x => x.Count).OrderBy(x => x));
            }
        }

        private sealed class CharacterCountPredicateIndex : AbstractIndexCreationTask<PrimitiveArrayItem, PrimitiveArrayCountPredicateIndex.Result>
        {
            public CharacterCountPredicateIndex(bool filtered)
            {
                if (filtered)
                    Map = items => from item in items
                                   select new PrimitiveArrayCountPredicateIndex.Result { Count = item.Tags.Where(tag => tag.Count() > 3).Count() };
                else
                    Map = items => from item in items
                                   select new PrimitiveArrayCountPredicateIndex.Result { Count = item.Tags.Count(tag => tag.Count() > 3) };

                Store(x => x.Count, FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // Typed selectors bind to the int elements of Range.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(-2, 3).Select(Math.Abs).Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Range(-2, 3).Select(Math.Abs)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 3).Select(RangeSelectors.Twice).Sum() == 6) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 3).Select((Func<int, int>)(x => x + 1)).Sum() == 6) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 3).Select((Func<int, int, int>)((x, i) => x + i)).Sum() == 6) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 2).SelectMany((Func<int, IEnumerable<int>>)(x => new[] { x, x })).Sum() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Select(Enumerable.Range(-2, 3), Math.Abs).Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(-1, 2).Select<int, int>(Math.Abs).Sum() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Method groups with int and object overloads keep their int overload.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Range(0, 1).Select(OverloadedSelectors.Label), \"int\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Range(0, 1).Select(OverloadedSelectors.LabelIndexed), \"int\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Select(Enumerable.Range(0, 1), OverloadedSelectors.Label), \"int\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A dynamic SelectMany lambda binds to the object extension as written, which converts date strings like other dynamic arrays.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).SelectMany(x => new object[] { \"2024-01-01T00:00:00.0000000Z\" }).First() is DateTime) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).SelectMany((Func<int, IEnumerable<string>>)(x => new[] { \"2024-01-01T00:00:00.0000000Z\" })).First() is string) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Dynamic lambdas and object selectors bind to object elements.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Range(0, 3).Select(x => (long)x), 2L)) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 3).Select((x, i) => x + i).Count() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 2).SelectMany(x => new[] { x, x }).Count() == 4) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Range(0, 3).Select(Convert.ToString), \"2\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 3).Select(RangeSelectors.Box).Count() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 3).Select((Func<object, object>)(x => x)).Count() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Select(Enumerable.Range(0, 3), x => (long)x), 2L)) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Select(Enumerable.Range(0, 3), Convert.ToString).Contains(\"2\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Select<object, object>(Enumerable.Range(0, 2), (Func<object, object>)(x => x)).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 2).Select<object, object>((Func<object, object>)(x => x)).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.SelectMany(Enumerable.Range(0, 1), (Func<dynamic, IEnumerable<dynamic>>)(x => new object[] { \"2024-01-01T00:00:00.0000000Z\" })).First() is string) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A nested Select keeps int elements inside a dynamic lambda that reads objects.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Range(0, 2).Select(x => Enumerable.Range(-2, 3).Select(Math.Abs).Sum() + x), 4)) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangeInsideRewrittenCallsKeepsSelectorElementType(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // Static calls keep the written argument order, also when the Range source is chosen.
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Select(selector: ArgumentProbe.Selector, source: Enumerable.Range(ArgumentProbe.Start, 2)).Count()) == \"selector,start:2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.SelectMany(selector: ArgumentProbe.ManySelector, source: Enumerable.Range(ArgumentProbe.Start, 2)).Count()) == \"selector,start:2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Select(selector: ArgumentProbe.IntSelector, source: Enumerable.Range(ArgumentProbe.Start, 2)).Sum()) == \"selector,start:1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Range(ArgumentProbe.Start, 2).Select(ArgumentProbe.Selector).Count()) == \"start,selector:2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Distinct keeps the written order of its source and comparer, also when the first one throws.
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Distinct(comparer: ArgumentProbe.Comparer, source: ArgumentProbe.Values).Count()) == \"comparer,source:2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Distinct(source: ArgumentProbe.Values, comparer: ArgumentProbe.Comparer).Count()) == \"source,comparer:2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Distinct(comparer: ArgumentProbe.ThrowingComparer, source: ArgumentProbe.Values).Count()) == \"comparer:InvalidOperationException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Distinct(source: ArgumentProbe.ThrowingValues, comparer: ArgumentProbe.Comparer).Count()) == \"source:InvalidOperationException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Run(() => Enumerable.Distinct(comparer: ArgumentProbe.Comparer, source: (dynamic)ArgumentProbe.Values).Count()) == \"comparer,source:2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void CallsInsideRewrittenCallsEvaluateArgumentsInWrittenOrder(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A Range returned by a selector keeps its int elements for typed consumers and gives objects to object delegates.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Sum((dynamic)Enumerable.First(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2)))) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Where((dynamic)Enumerable.First(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2))), (Func<dynamic, bool>)(x => x > 1))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count((dynamic)Enumerable.First(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2))), (Func<dynamic, bool>)(x => x > 1)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Where((dynamic)Enumerable.First(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2))), (Func<dynamic, int, bool>)((x, i) => i == 1))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Where((dynamic)Enumerable.First(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2))), (Func<int, bool>)(x => x > 1))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The same value seen through one alias by both kinds of consumers, and after materialization.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(new object[] { Enumerable.First(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2))) }, r => Enumerable.Sum((dynamic)r) == 3 && Enumerable.Count(Enumerable.Where((dynamic)r, (Func<dynamic, bool>)(x => x > 1))) == 1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Where((dynamic)Enumerable.First(Enumerable.ToArray(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2)))), (Func<dynamic, bool>)(x => x > 1))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Sum((dynamic)Enumerable.First(Enumerable.ToList(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2))))) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Reference sequences, dynamic objects and nulls bind as before (an object delegate binds reference elements as objects);
        // the source is evaluated once.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct(Enumerable.Where((dynamic)new[] { new IdentityTuple(1), new IdentityTuple(1) }, (Func<dynamic, bool>)(x => true)))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Count(Enumerable.Where(ExplicitSequence.Create(), (Func<dynamic, bool>)(x => true))) == 0)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Count(Enumerable.Where((dynamic)null, (Func<dynamic, bool>)(x => true))) == 0)) == \"ArgumentNullException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => CountProbe.Reset() && Enumerable.Count(Enumerable.Where(CountProbe.Next(), (Func<dynamic, bool>)(x => true))) == 3 && CountProbe.Calls == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ValuesInsideRewrittenCallsServeTypedAndObjectDelegates(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // Index code may use the names of the generated source choices: a range variable named DynamicEnumerable keeps its method,
        // a method named ChooseRangeSource keeps its typed lambdas, and a range variable with the name the generated Range source
        // parameter would get (rangeSource and the position of the Select call in this map) is not hidden by it. A call of the Range
        // source helper with a lambda that does not pass a source stays a call.
        [RavenData("let DynamicEnumerable = new GeneratedNameProjection()", "DynamicEnumerable.ObjectDelegateSource(1) == 11 && DynamicEnumerable.Calls == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("", "GeneratedNameProbe.ChooseRangeSource(x => new[] { 1, 2 }.Select(y => y + 1).Count()) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("", "DynamicEnumerable.RangeSource.Instance.ChooseRangeSource(x => 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("let rangeSource128 = (Func<int, int>)(v => v + 1)", "Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).Select((Func<dynamic, dynamic>)(x => rangeSource128(0))).First() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void GeneratedSourceChoicesKeepNamesWrittenInIndex(Options options, string lets, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["GeneratedNames.cs"] = GeneratedNameSources }, lets);
        }

        private const string GeneratedNameSources = """
            using System;
            public sealed class GeneratedNameProjection
            {
                internal int Calls;
                internal int ObjectDelegateSource(int value)
                {
                    Calls++;
                    return value + 10;
                }
            }
            public static class GeneratedNameProbe
            {
                internal static int ChooseRangeSource(Func<int, int> callback) => callback(0);
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        // Index sources may declare types with the name of the helper type. A type with its full name keeps the calls of its methods.
        // A type with its name in the namespace of the index keeps the lambdas of its calls, also of a call with the shape of the
        // generated Range source choice. Neither type captures the generated Range source choice nor the object delegate source,
        // also next to a call of the index type.
        [RavenData(true, "DynamicEnumerable.ObjectDelegateSource(1) == 11", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "DynamicEnumerable.RangeSource.Instance.ChooseRangeSource(x => new[] { 1, 2 }.Select(y => y + 1).Count()) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "DynamicEnumerable.RangeSource.Instance.ChooseRangeSource(callback: x => new[] { 1, 2 }.Select(y => y + 1).Count()) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "DynamicEnumerable.RangeSource.Instance.ChooseRangeSource((x) => new[] { 1, 2 }.Select(y => y + 1).Count()) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).Select((Func<dynamic, dynamic>)(x => x)).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).Select((Func<dynamic, dynamic>)(x => x)).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Count(item.Tags, tag => Enumerable.Count((dynamic)new[] { 1, 2 }, (Func<dynamic, bool>)(x => x > 1)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(item.Tags, tag => Enumerable.Count((dynamic)new[] { 1, 2 }, (Func<dynamic, bool>)(x => x > 1)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "DynamicEnumerable.ObjectDelegateSource(1) == 11 && Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).Select((Func<dynamic, dynamic>)(x => x)).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void GeneratedSourceChoicesIgnoreIndexTypesWithHelperNames(Options options, bool fullName, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["HelperNames.cs"] = fullName ? HelperFullNameSources : HelperIndexNamespaceSources });
        }

        private const string HelperFullNameSources = """
            namespace Raven.Server.Documents.Indexes.Static.Linq
            {
                public static class DynamicEnumerable
                {
                    internal static int ObjectDelegateSource(int value) => value + 10;
                }
            }
            """;

        private const string HelperIndexNamespaceSources = """
            namespace Raven.Server.Documents.Indexes.Static.Generated
            {
                public static class DynamicEnumerable
                {
                    public static class RangeSource
                    {
                        public static readonly Picker Instance = new Picker();
                    }
                }

                public sealed class Picker
                {
                    internal int ChooseRangeSource(System.Func<int, int> callback) => callback(0);
                }
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        // Static sources keep their element type.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct(new[] { -1, 1, 1 }).Select(Math.Abs).Sum() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct(new[] { 1, 1, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct(new[] { 1, 1, 2 }, EqualityComparer<int>.Default).Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct(comparer: EqualityComparer<int>.Default, source: new[] { 1, 1, 2 }).Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct(new[] { \"a\", \"A\" }, StringComparer.OrdinalIgnoreCase).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Stored values keep the object view, including characters of a stored string.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct(tag)) == 4) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Distinct(item.Tags).Select(Convert.ToString), \"alpha\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct(item.Tags).Select(x => x).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => (from x in Enumerable.Distinct(item.Tags) select x).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct(item.Tags, EqualityComparer<object>.Default).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Reference elements keep their own equality: the element type is chosen before the result is exposed as objects.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct((dynamic)new[] { new IdentityTuple(1), new IdentityTuple(1) })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct((dynamic)new[] { new TracedTuple(1), new TracedTuple(1) })) == 1 && TracedTuple.TypedEqualsCalls > 0) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct((dynamic)new[] { new IdentityTuple(1), new IdentityTuple(1) }, EqualityComparer<object>.Default)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctInsideRewrittenCallsKeepsSourceElementType(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A consumer of a specific sequence type gets the Distinct result of a dynamic source with the element type of the source,
        // through an explicit type argument, a cast or the parameter of an internal method, with the comparer in either position.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 3, 2 }, (dynamic)new ParityComparer())) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct(comparer: (dynamic)new ParityComparer(), source: (dynamic)new[] { 1, 3, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Order(() => Enumerable.Count<int>(Enumerable.Distinct(comparer: (dynamic)ArgumentProbe.IntComparer, source: (dynamic)ArgumentProbe.IntValues))) == \"comparer,source\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.MostEvaluations(() => Enumerable.Count<int>(Enumerable.Distinct(comparer: (dynamic)ArgumentProbe.IntComparer, source: (dynamic)ArgumentProbe.IntValues))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Sum((IEnumerable<int>)Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TypedConsumers.Sum(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A cast to dynamic only defers the binding of the consumer, which still gets the result with the element type of the source,
        // or as objects when the element type matches none of its typed overloads.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Sum((dynamic)Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Overloads.Pick((dynamic)Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Overloads.Pick((dynamic)Enumerable.Distinct((dynamic)new[] { 1L, 1L, 2L })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<IdentityTuple>(Enumerable.Distinct((dynamic)new[] { new IdentityTuple(1), new IdentityTuple(1) })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The result stays deferred, and null sources and enumeration errors surface as before.
        [RavenData("Enumerable.Count(item.Tags, tag => DeferredProbe.Run((Func<IEnumerable<int>>)(() => (IEnumerable<int>)Enumerable.Distinct((dynamic)DeferredProbe.Values()))) == \"source,returned,enumerated,2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Count<int>(Enumerable.Distinct((dynamic)null)) == 0)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Count<int>(Enumerable.Distinct((dynamic)DeferredProbe.Throwing())) == 0)) == \"InvalidOperationException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The same sources keep the object view for extension methods, method groups and query syntax, and a typed comparer
        // keeps the typed overload.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }).Select(Convert.ToString), \"2\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => (from x in Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }) select x).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 3, 2 }, (dynamic)new ParityComparer()).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Order(() => Enumerable.Distinct(comparer: (dynamic)ArgumentProbe.IntComparer, source: (dynamic)ArgumentProbe.IntValues).Count()) == \"comparer,source\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.MostEvaluations(() => Enumerable.Distinct(comparer: (dynamic)ArgumentProbe.IntComparer, source: (dynamic)ArgumentProbe.IntValues).Count()) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 3, 2 }, (IEqualityComparer<int>)new ParityComparer())) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => DeferredProbe.RunObjects((Func<IEnumerable<object>>)(() => Enumerable.Distinct((dynamic)DeferredProbe.Values()))) == \"source,returned,enumerated,2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Count(Enumerable.Distinct((dynamic)null)) == 0)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Count(Enumerable.Distinct((dynamic)DeferredProbe.Throwing())) == 0)) == \"InvalidOperationException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctInsideRewrittenCallsKeepsDynamicResultForTypedConsumers(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A constructor parameter, a declared local, field or property, or the return type of a typed delegate takes the Distinct
        // result of a dynamic source with the element type of the source.
        [RavenData("Enumerable.Count(item.Tags, tag => new List<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })).Count == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => new List<int>((Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }))).Count == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]        [RavenData("Enumerable.Count(item.Tags, tag => TypedConsumers.SumAll(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), new[] { 4 }) == 7) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ((Func<int>)(() => { IEnumerable<int> values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); return values.Sum(); }))() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ((Func<int>)(() => { IEnumerable<int> values = null; values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); return values.Sum(); }))() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => new Holder { Values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }) }.Values.Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => new Holder { Items = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }) }.Items.Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Sum(((Func<IEnumerable<int>>)(() => Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })))()) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Sum(((Func<IEnumerable<int>>)(() => { return Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); }))()) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // An implicitly typed local keeps the object result for extension methods, while an implicitly typed or object local whose
        // every reference needs the element type of the source, as a typed consumer or a cast to a typed sequence does, takes that result.
        [RavenData("Enumerable.Count(item.Tags, tag => ((Func<int>)(() => { var values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); return values.Count(); }))() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ((Func<int>)(() => { var values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); return Enumerable.Sum(values); }))() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ((Func<int>)(() => { object values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); return Enumerable.Sum((IEnumerable<int>)values); }))() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ((Func<int>)(() => { object values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); return ((IEnumerable<object>)values).Count(); }))() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A local that is also read as objects keeps the object result for all its references, also for a type test.
        [RavenData("Enumerable.Count(item.Tags, tag => ((Func<bool>)(() => { var values = Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }); return values is IEnumerable<object> && values.Count() == 2; }))()) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The overload is chosen at runtime by the element type of the source, as the call written in the index did,
        // also when an overload taking objects binds at compile time.
        [RavenData("Enumerable.Count(item.Tags, tag => Overloads.Pick(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Overloads.Pick(Enumerable.Distinct((dynamic)new[] { \"a\", \"a\" })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Overloads.PickElement(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A call that cannot be dispatched dynamically, e.g. with a lambda argument, keeps the object result.
        [RavenData("Enumerable.Count(item.Tags, tag => Overloads.PickWith(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), x => x) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A nested Distinct gets the element type of its source for the comparer or the consumer of the outer call,
        // with the arguments evaluated once in the order they were written.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), (IEqualityComparer<int>)EqualityComparer<int>.Default)) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct(comparer: (IEqualityComparer<int>)new ParityComparer(), source: Enumerable.Distinct((dynamic)new[] { 1, 3, 2 }))) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct(Enumerable.Distinct((dynamic)new[] { 1, 3, 2 }), (IEqualityComparer<int>)new ParityComparer()).Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct(Enumerable.Distinct((dynamic)new[] { 1, 3, 2 }), (dynamic)new ParityComparer())) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }))) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.Order(() => Enumerable.Count<int>(Enumerable.Distinct(comparer: ArgumentProbe.IntComparer, source: Enumerable.Distinct((dynamic)ArgumentProbe.IntValues)))) == \"comparer,source\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArgumentProbe.MostEvaluations(() => Enumerable.Count<int>(Enumerable.Distinct(comparer: ArgumentProbe.IntComparer, source: Enumerable.Distinct((dynamic)ArgumentProbe.IntValues)))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The source of an Enumerable call with a typed delegate gets the dynamic result, while a delegate reading objects keeps
        // the object result.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), (Func<int, bool>)(x => x > 1)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), (Func<dynamic, bool>)(x => x > 1)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A type test or a typed pattern on the result sees the element type of the source, while a var pattern keeps the object result.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int>) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) as IEnumerable<int> != null) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<object>) == 0", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<long>) == 0", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { \"a\" }) is IEnumerable<object>) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }) is var values && values.Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }) is IEnumerable<int> values && values.Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }) is IEnumerable<long> values && values.Sum() == 3) == 0", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is not IEnumerable<int>) == 0", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is not (IEnumerable<int>)) == 0", SearchEngineMode = RavenSearchEngineMode.All)]
        // Each branch of a conditional takes the type test of the conditional, and keeps the object result for an extension method.
        [RavenData("Enumerable.Count(item.Tags, tag => (tag == \"alpha\" ? Enumerable.Distinct((dynamic)new[] { 3 }) : Enumerable.Distinct((dynamic)new[] { 1, 2 })) is IEnumerable<int>) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => (tag == \"alpha\" ? Enumerable.Distinct((dynamic)new[] { 3 }) : Enumerable.Distinct((dynamic)new[] { 1, 2 })).Count() == (tag == \"alpha\" ? 1 : 2)) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctInsideRewrittenCallsKeepsDynamicResultForTypedContexts(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A property pattern, a combined type pattern or a switch expression on the result sees the element type of the source,
        // the same as the plain type test above.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> { }) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> or IEnumerable<long>) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> and not null) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => (Enumerable.Distinct((dynamic)new[] { 1, 2 }) switch { IEnumerable<int> values => values.Sum() == 3, _ => false })) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<object> or IEnumerable<long>) == 0", SearchEngineMode = RavenSearchEngineMode.All)]
        // A var pattern in a combined pattern or in a switch arm keeps the object result, which its capture binds to.
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is var values and not IEnumerable<long> && values.Count() == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => (Enumerable.Distinct((dynamic)new[] { 1, 2 }) switch { IEnumerable<long> => false, var values => values.Count() == 2 })) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void NestedDistinctComplexTypePatternsKeepRuntimeElementType(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Count(item.Tags, tag => new List<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })).Count == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Overloads.Pick(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), (IEqualityComparer<int>)EqualityComparer<int>.Default)) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), (Func<int, bool>)(x => x > 1)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int>) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctTypedContextsExecuteInReduce(Options options, string expression)
        {
            AssertReduceExpression(options, expression, new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        // A consumer that cannot bind the dynamic result keeps the object result without changing the result of another consumer.
        public void DistinctConsumersInOneIndexAreSelectedSeparately(Options options)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/Consumers";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps =
                {
                    "from item in docs.PrimitiveArrayItems select new { " +
                    "Typed = Enumerable.Count(item.Tags, tag => Overloads.Pick(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 1) == 2, " +
                    "Facade = Enumerable.Count(item.Tags, tag => Overloads.PickWith(Enumerable.Distinct((dynamic)new[] { \"a\", \"a\" }), x => x) == 2) == 2 }"
                },
                Fields =
                {
                    ["Typed"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["Facade"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
                },
                AdditionalSources = new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ConsumersResult>($"from index '{indexName}' select Typed, Facade").Single();
                Assert.True(result.Facade, "Facade");
                Assert.True(result.Typed, "Typed");
            }
        }

        private sealed class ConsumersResult
        {
            public bool Typed { get; set; }
            public bool Facade { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // The element type of the source selects the overload, as the call written in the index did, and a source that fits
        // incomparable overloads fails as ambiguous (see DistinctConsumerOfIncomparableTypedOverloadsFailsAsAmbiguous).
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousConsumers.Pick(Enumerable.Distinct((dynamic)new[] { 1, 1 })) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousConsumers.Pick((IEnumerable<object>)Enumerable.Distinct((dynamic)new[] { new Both() })) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => new AmbiguousHolder(Enumerable.Distinct((dynamic)new[] { new LeftOnly() })).Overload == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The receiver and the arguments are evaluated once, in the order they were written, and the source is not enumerated
        // before the consumer.
        [RavenData("Enumerable.Count(item.Tags, tag => TracedConsumer.Run(() => TracedConsumer.Create().Pick(TracedConsumer.Step(\"before\"), Enumerable.Distinct((dynamic)TracedConsumer.LeftOnlyValues()), TracedConsumer.Step(\"after\"))) == \"receiver,before,source,after,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A consumer without a result keeps the dynamic call.
        [RavenData("Enumerable.Count(item.Tags, tag => TracedConsumer.RunAction(() => TracedConsumer.Create().Record(Enumerable.Distinct((dynamic)TracedConsumer.LeftOnlyValues()))) == \"receiver,source,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerOverloadsOfIncomparableSequences(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerOverloadsOfIncomparableSequencesExecuteInReduce(Options options, string expression)
        {
            AssertReduceExpression(options, expression, new Dictionary<string, string> { ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A result whose elements fit incomparable typed overloads binds as the dynamic call does, so the runtime binder reports the
        // ambiguity and no other overload is chosen instead. Each expression holds for the object overload, which is not called.
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() })) == 3) == 2", "AmbiguousConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => new AmbiguousHolder(Enumerable.Distinct((dynamic)new[] { new Both() })).Overload == 3) == 2", "AmbiguousHolder.AmbiguousHolder", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TracedConsumer.Run(() => TracedConsumer.Create().Pick(TracedConsumer.Step(\"before\"), Enumerable.Distinct((dynamic)TracedConsumer.BothValues()), TracedConsumer.Step(\"after\"))) == \"receiver,before,source,after,consumer3\") == 2", "TracedConsumer.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => DelegateConsumers.Pick(Enumerable.Distinct((dynamic)DelegateConsumers.BothValues())) == 3) == 2", "DelegateConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => PairConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), Enumerable.Distinct((dynamic)new[] { new Both() })) == 3) == 2", "PairConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => VoidMemberConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() })) == 3) == 2", "VoidMemberConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => GenericMemberConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() })) == 3) == 2", "GenericMemberConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ConversionProbe.Run(() => ConversionConsumers.Pick(1, Enumerable.Distinct((dynamic)ConversionProbe.BothValues()))) == \"convert,source,consumer3\") == 2", "ConversionConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        // The other arguments leave the typed overloads equally good, or each better at a different position.
        [RavenData("Enumerable.Count(item.Tags, tag => SplitConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), 1, 1) == 3) == 2", "SplitConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => EqualConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), 1) == 3) == 2", "EqualConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => EqualConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (dynamic)1) == 3) == 2", "EqualConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => CrossConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 3) == 2", "CrossConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousHandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{1}\") == 3) == 2", "AmbiguousHandlerConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (1L, (long)\"ab\".Length)) == 3) == 2", "TupleConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => ArmDropConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), 1) == 2) == 2", "ArmDropConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (1L, (long)\"ab\".Length)) == 3) == 2", "NullableTupleConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{HandlerTrace.Step(1)}\")) == \"handler,step,consumer3\") == 2", "FacadeConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick(Enumerable.Distinct((dynamic)new TracedSource<Both>()), $\"{HandlerTrace.Step(1)}\")) == \"source,handler,step,consumer3\") == 2", "FacadeConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerOfIncomparableTypedOverloadsFailsAsAmbiguous(Options options, string expression, string member)
        {
            AssertIndexingFailsAsAmbiguous(options, expression, member, inReduce: false);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Count(item.Tags, tag => AmbiguousConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() })) == 3) == 2", "AmbiguousConsumers.Pick", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerOfIncomparableTypedOverloadsFailsAsAmbiguousInReduce(Options options, string expression, string member)
        {
            AssertIndexingFailsAsAmbiguous(options, expression, member, inReduce: true);
        }

        private void AssertIndexingFailsAsAmbiguous(Options options, string expression, string member, bool inReduce)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/AmbiguousConsumer";
            var definition = new IndexDefinition
            {
                Name = indexName,
                AdditionalSources = new Dictionary<string, string>
                {
                    ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources,
                    ["BetterConsumers.cs"] = BetterConsumerSources,
                    ["ConsumerPlans.cs"] = ConsumerPlanSources,
                    ["ConversionConsumers.cs"] = ConversionConsumerSources,
                    ["DelegateConsumers.cs"] = DelegateConsumerSources,
                    ["HandlerConsumers.cs"] = HandlerConsumerSources
                },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            };
            if (inReduce)
            {
                definition.Maps.Add("from item in docs.PrimitiveArrayItems select new { item.Group, item.Tags, Found = false }");
                definition.Reduce = "from result in results group result by result.Group into grouped let item = grouped.First() select new { Group = grouped.Key, item.Tags, Found = " + expression + " }";
            }
            else
            {
                definition.Maps.Add("from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }");
            }

            store.Maintenance.Send(new PutIndexesOperation(definition));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Group = "tags", Tags = new[] { "alpha", "b" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            // The error of the batch may be visible after the index stops being stale.
            Indexes.WaitForIndexingErrors(store, new[] { indexName }, errorsShouldExists: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            var error = Assert.Single(errors).Error;
            Assert.Contains("RuntimeBinderException: The call is ambiguous between the following methods or properties: '" + member + "(", error);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A delegate of a covariant result converts to the delegates of the views of its result, so a source of delegates selects the
        // overload by its element type as well.
        [RavenData("Enumerable.Count(item.Tags, tag => DelegateConsumers.Pick(Enumerable.Distinct((dynamic)DelegateConsumers.LeftOnlyValues())) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => DelegateConsumers.Pick(Enumerable.Distinct((dynamic)DelegateConsumers.RightOnlyValues())) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => DelegateConsumers.Pick((IEnumerable<object>)System.Linq.Enumerable.Distinct((dynamic)DelegateConsumers.BothValues())) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The framework Distinct keeps the dynamic call, which fails as ambiguous.
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => DelegateConsumers.Pick(System.Linq.Enumerable.Distinct((dynamic)DelegateConsumers.BothValues())) == 3)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerOverloadsOfIncomparableDelegateSequences(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string>
                {
                    ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["BetterConsumers.cs"] = BetterConsumerSources, ["DelegateConsumers.cs"] = DelegateConsumerSources
                });
        }

        // The delegates are created by a typed factory in the sources, so no lambda or dynamic argument of the index creates them; the
        // methods are internal, so their signatures are kept as declared.
        private const string DelegateConsumerSources = """
            using System;
            using System.Collections.Generic;
            public static class DelegateConsumers
            {
                internal static Func<Both>[] BothValues() => new Func<Both>[] { () => new Both() };
                internal static Func<LeftOnly>[] LeftOnlyValues() => new Func<LeftOnly>[] { () => new LeftOnly() };
                internal static Func<RightOnly>[] RightOnlyValues() => new Func<RightOnly>[] { () => new RightOnly() };
                internal static int Pick(IEnumerable<Func<ILeft>> values) => 1;
                internal static int Pick(IEnumerable<Func<IRight>> values) => 2;
                internal static int Pick(IEnumerable<object> values) => 3;
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        // The results passed to one consumer select its overload together, as the call written in the index did.
        [RavenData("Enumerable.Count(item.Tags, tag => PairConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => PairConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() }), Enumerable.Distinct((dynamic)new[] { new RightOnly() })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => PairConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => new PairHolder(Enumerable.Distinct((dynamic)new[] { new Both() }), Enumerable.Distinct((dynamic)new[] { new LeftOnly() })).Overload == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The receiver, the arguments, the sources and the comparer are evaluated once, in the order they were written, and no
        // source is enumerated before the consumer.
        [RavenData("Enumerable.Count(item.Tags, tag => TracedPairs.Run(() => TracedPairs.Create().Pick(TracedPairs.Step(\"before\"), Enumerable.Distinct((dynamic)TracedPairs.BothValues(), (dynamic)TracedPairs.Comparer()), TracedPairs.Step(\"middle\"), Enumerable.Distinct((dynamic)TracedPairs.LeftOnlyValues()), TracedPairs.Step(\"after\"))) == \"receiver,before,source,comparer,middle,source,after,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctResultsOfOneConsumerSelectItsOverloadTogether(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["ConsumerPlans.cs"] = ConsumerPlanSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // Only the overloads that take all the arguments of the call are choices: the other arguments select among the overloads
        // a result fits, and members that do not take the arguments change nothing.
        [RavenData("Enumerable.Count(item.Tags, tag => DiscriminatedConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), 1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => DiscriminatedConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), \"kind\") == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A number cast to dynamic is a value of its type, so it selects among the overloads as in the call written in the index,
        // with the sources themselves, and the object results bind their overload.
        [RavenData("Enumerable.Count(item.Tags, tag => DiscriminatedConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (dynamic)1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("System.Linq.Enumerable.Count(item.Tags, tag => DiscriminatedConsumers.Pick(System.Linq.Enumerable.Distinct((dynamic)new[] { new Both() }), (dynamic)1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => DiscriminatedConsumers.Pick((IEnumerable<object>)System.Linq.Enumerable.Distinct((dynamic)new[] { new Both() }), (dynamic)1) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => VoidMemberConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => GenericMemberConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerOverloadsAreChosenForTheWholeCall(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["ConsumerPlans.cs"] = ConsumerPlanSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // The selected code does not depend on the names written in the index or in its sources.
        [RavenData("Enumerable.Count(item.Tags, tag => PrefixConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new PrefixLeft() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, __distinctView0 => AmbiguousConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, __distinctCapture0_0 => TracedConsumer.Create().Pick(TracedConsumer.Step(\"before\"), Enumerable.Distinct((dynamic)TracedConsumer.LeftOnlyValues()), TracedConsumer.Step(\"after\")) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, __distinctCapture0_0 => TracedConsumer.Run(() => TracedConsumer.Create().Pick(TracedConsumer.Step(\"before\"), Enumerable.Distinct((dynamic)TracedConsumer.LeftOnlyValues()), TracedConsumer.Step(\"after\"))) == \"receiver,before,source,after,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerSelectionKeepsIndexNames(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["ConsumerPlans.cs"] = ConsumerPlanSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A result that may match a typed overload selects it once the source is evaluated.
        [RavenData("ConversionConsumers.Pick(1, Enumerable.Distinct((dynamic)ConversionProbe.LeftOnlyValues()))", "source,convert,consumer1", SearchEngineMode = RavenSearchEngineMode.All)]
        // The dynamic call infers an element class the helper cannot access as its accessible base class, without the interfaces
        // of the element, so a result of a declared interface may match a typed overload only.
        [RavenData("ConversionConsumers.Pick(1, Enumerable.Distinct((dynamic)Factory.BothValues()))", "source,convert,consumer1", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerConvertsArgumentsAsTheResultsBind(Options options, string call, string trace)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ConversionOrder";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = Enumerable.Count(item.Tags, tag => ConversionProbe.Run(() => " + call + ") == \"" + trace + "\") == 2, Trace = ConversionProbe.Observed() }" },
                AdditionalSources = new Dictionary<string, string> { ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["ConversionConsumers.cs"] = ConversionConsumerSources },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Trace"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var index = (MapIndex)Databases.GetDocumentDatabaseInstanceFor(store).GetAwaiter().GetResult().IndexStore.GetIndex(indexName);
            Output.WriteLine(index._compiled.Source);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), call + Environment.NewLine + string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<TracedResult>($"from index '{indexName}' select Found, Trace").Single();
                Assert.True(result.Found, call + " traced " + result.Trace);
            }
        }

        private sealed class TracedResult
        {
            public bool Found { get; set; }

            public string Trace { get; set; }
        }

        // Consumers of the element types of AmbiguousConsumerSources whose first argument converts with a trace.
        private const string ConversionConsumerSources = """
            using System;
            using System.Collections.Generic;
            public interface IBoth : ILeft, IRight { }
            public class PublicLeft : ILeft { }
            public static class Factory
            {
                private sealed class HiddenBoth : PublicLeft, IBoth { }
                internal static IEnumerable<IBoth> BothValues()
                {
                    ConversionProbe.Mark("source");
                    return new HiddenBoth[] { new HiddenBoth() };
                }
            }
            public sealed class BeforeToken
            {
                public static implicit operator BeforeToken(int value)
                {
                    ConversionProbe.Mark("convert");
                    return new BeforeToken();
                }
            }
            public static class ConversionProbe
            {
                [ThreadStatic]
                private static List<string> _trace;
                [ThreadStatic]
                private static List<string> _observed;
                public static void Mark(string step)
                {
                    if (_trace == null)
                        _trace = new List<string>();
                    _trace.Add(step);
                }
                public static string Run(Func<int> consume)
                {
                    _trace = new List<string>();
                    consume();
                    var result = string.Join(",", _trace);
                    _trace = null;
                    if (_observed == null)
                        _observed = new List<string>();
                    _observed.Add(result);
                    return result;
                }
                public static string Observed()
                {
                    var result = _observed == null ? string.Empty : string.Join("|", _observed);
                    _observed = null;
                    return result;
                }
                internal static IEnumerable<Both> BothValues() => Values(new Both());
                internal static IEnumerable<LeftOnly> LeftOnlyValues() => Values(new LeftOnly());
                private static IEnumerable<T> Values<T>(T value)
                {
                    Mark("source");
                    return Enumerate(value);
                }
                private static IEnumerable<T> Enumerate<T>(T value)
                {
                    Mark("enumerated");
                    yield return value;
                }
                internal static int Consume(int overload)
                {
                    Mark("consumer" + overload);
                    return overload;
                }
            }
            public static class ConversionConsumers
            {
                internal static int Pick(BeforeToken before, IEnumerable<ILeft> values) => ConversionProbe.Consume(1);
                internal static int Pick(BeforeToken before, IEnumerable<IRight> values) => ConversionProbe.Consume(2);
                internal static int Pick(BeforeToken before, IEnumerable<object> values) => ConversionProbe.Consume(3);
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        // The overload is the best one for the whole call, as for the call written in the index: the other arguments make one of
        // the overloads a result fits better.
        [RavenData("Enumerable.Count(item.Tags, tag => BetterConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), 1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BetterConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (dynamic)1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BetterConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), 1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BetterConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() }), 1) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BetterConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), 1L) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => SplitConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), 1, 1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => SplitConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() }), 1, 1) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => CrossConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), Enumerable.Distinct((dynamic)new[] { new object() })) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => CrossConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new object() }), Enumerable.Distinct((dynamic)new[] { new LeftOnly() })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The same calls with the sources themselves, which no result is selected for: the dynamic call binds the better overload or
        // fails as ambiguous, and the object results bind their overload.
        [RavenData("Enumerable.Count(item.Tags, tag => BetterConsumers.Pick((dynamic)new[] { new Both() }, 1) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => SplitConsumers.Pick((dynamic)new[] { new Both() }, 1, 1) == 3)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => CrossConsumers.Pick((dynamic)new[] { new LeftOnly() }, (dynamic)new[] { new LeftOnly() }) == 3)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => EqualConsumers.Pick((dynamic)new[] { new Both() }, (dynamic)1) == 3)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => CrossConsumers.Pick((IEnumerable<object>)new[] { new LeftOnly() }, (IEnumerable<object>)new[] { new LeftOnly() }) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerSelectsTheBestOverloadOfTheWholeCall(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["BetterConsumers.cs"] = BetterConsumerSources });
        }

        // Consumers of the element types of AmbiguousConsumerSources whose overloads differ in other arguments or at several positions;
        // the methods are internal, so their signatures are kept as declared.
        private const string BetterConsumerSources = """
            using System;
            using System.Collections.Generic;
            public static class BetterConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, int kind) => 1;
                internal static int Pick(IEnumerable<IRight> values, long kind) => 2;
                internal static int Pick(IEnumerable<object> values, object kind) => 3;
            }
            public static class SplitConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, int first, long second) => 1;
                internal static int Pick(IEnumerable<IRight> values, long first, int second) => 2;
                internal static int Pick(IEnumerable<object> values, object first, object second) => 3;
            }
            public static class EqualConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, int kind) => 1;
                internal static int Pick(IEnumerable<IRight> values, int kind) => 2;
                internal static int Pick(IEnumerable<object> values, object kind) => 3;
            }
            public static class CrossConsumers
            {
                internal static int Pick(IEnumerable<ILeft> first, IEnumerable<object> second) => 1;
                internal static int Pick(IEnumerable<object> first, IEnumerable<ILeft> second) => 2;
                internal static int Pick(IEnumerable<object> first, IEnumerable<object> second) => 3;
            }
            public static class BaseProbe
            {
                internal static string ExceptionType(Func<bool> call)
                {
                    try
                    {
                        call();
                        return null;
                    }
                    catch (Exception e)
                    {
                        return e.GetType().Name;
                    }
                }
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        // The other arguments are values of their own types, as for the call written in the index: an interpolated string is a
        // string, which converts to a handler as a string does, and a tuple literal is a tuple of its element types, while constants
        // convert as they are written. Each argument is evaluated once, after the results written before it.
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{1}\") == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), $\"{1}\") == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() }), $\"{1}\") == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (string)$\"{1}\") == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), HandlerTrace.Text) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => LeadingHandlerConsumers.Pick($\"{1}\", Enumerable.Distinct((dynamic)new[] { new Both() })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => HandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{HandlerTrace.Step(1)}\")) == \"step,consumer2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => HandlerConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), $\"{HandlerTrace.Step(1)}\")) == \"step,implicit,handler,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), (1L, (long)\"ab\".Length)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // A number cast to dynamic is a value of its type also when it is written as a constant, so it takes no overload by a constant
        // conversion, and it is evaluated once, after the results written before it.
        [RavenData("Enumerable.Count(item.Tags, tag => KindConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (dynamic)1) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => KindConsumers.Pick(Enumerable.Distinct((dynamic)new TracedSource<Both>()), (dynamic)HandlerTrace.Step(1))) == \"source,step,consumer2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => HandlerTrace.Consume(BetterConsumers.Pick(Enumerable.Distinct((dynamic)new TracedSource<Both>()), (dynamic)HandlerTrace.Step(1)))) == \"source,step,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The same calls with the sources themselves, which no result is selected for: the dynamic call binds the better overload or
        // fails as ambiguous.
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerConsumers.Pick((dynamic)new[] { new Both() }, $\"{1}\") == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => HandlerConsumers.Pick((dynamic)new[] { new Both() }, $\"{HandlerTrace.Step(1)}\")) == \"step,consumer2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => HandlerConsumers.Pick((dynamic)new[] { new LeftOnly() }, $\"{HandlerTrace.Step(1)}\")) == \"step,implicit,handler,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => LeadingHandlerConsumers.Pick($\"{1}\", (dynamic)new[] { new Both() }) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => AmbiguousHandlerConsumers.Pick((dynamic)new[] { new Both() }, $\"{1}\") == 3)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The runtime binder converts a tuple to another tuple type by no conversion.
        [RavenData("Enumerable.Count(item.Tags, tag => TupleConsumers.Pick((dynamic)new[] { new Both() }, (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, (1L, (long)\"ab\".Length)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => TupleConsumers.Pick((dynamic)new[] { new Both() }, (1L, (long)\"ab\".Length)) == 3)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => ArmDropConsumers.Pick((dynamic)new[] { new LeftOnly() }, 1) == 2)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => KindConsumers.Pick((dynamic)new[] { new Both() }, (dynamic)1) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => KindConsumers.Pick((dynamic)new[] { new LeftOnly() }, (dynamic)1) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => KindConsumers.Pick((dynamic)new TracedSource<Both>(), (dynamic)HandlerTrace.Step(1))) == \"source,step,consumer2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerTakesOtherArgumentsAsValuesOfTheirTypes(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string>
                {
                    ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["BetterConsumers.cs"] = BetterConsumerSources, ["HandlerConsumers.cs"] = HandlerConsumerSources
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A value converts to a nullable type by its conversion to the underlying type, so a tuple value of other element types takes
        // no nullable tuple parameter, while the other conversions to a nullable type apply as for the call written in the index.
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() }), (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), ((int, int)?)(1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), (1L, (long)\"ab\".Length)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), ((long, long)?)(1L, (long)\"ab\".Length)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), null) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), new LongPair()) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableNumberConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), \"ab\".Length) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The same calls with the sources themselves, which no result is selected for.
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new RightOnly() }, (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new Both() }, (1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, ((int, int)?)(1, \"ab\".Length)) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, (1L, (long)\"ab\".Length)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => BaseProbe.ExceptionType((Func<bool>)(() => NullableTupleConsumers.Pick((dynamic)new[] { new Both() }, (1L, (long)\"ab\".Length)) == 3)) == \"RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, ((long, long)?)(1L, (long)\"ab\".Length)) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, null) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableTupleConsumers.Pick((dynamic)new[] { new LeftOnly() }, new LongPair()) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => NullableNumberConsumers.Pick((dynamic)new[] { new LeftOnly() }, \"ab\".Length) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerTakesNullableArgumentsAsTheBinderDoes(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string>
                {
                    ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["BetterConsumers.cs"] = BetterConsumerSources, ["HandlerConsumers.cs"] = HandlerConsumerSources
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // The arguments after the last result are evaluated once the view is selected: for a typed view as values of their types, as
        // for the dynamic call, and for the object results as written, so an interpolated string converts to a handler constructed
        // before its holes, as in the call written in the index. The source, each argument and the consumer run once, in written order.
        // A result that fits incomparable typed overloads binds as the dynamic call does: its interpolated string is evaluated as a
        // string before the binder fails as ambiguous.
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), $\"{HandlerTrace.Step(1)}\")) == \"step,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new RightOnly() }), $\"{HandlerTrace.Step(1)}\")) == \"step,consumer2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick(Enumerable.Distinct((dynamic)new TracedSource<LeftOnly>()), $\"{HandlerTrace.Step(1)}\")) == \"source,step,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => FacadeConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{HandlerTrace.Step(1) / HandlerTrace.Step(0)}\")) == \"step,step,DivideByZeroException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => FacadeConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), $\"{HandlerTrace.Step(1) / HandlerTrace.Step(0)}\")) == \"step,step,DivideByZeroException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => ThrowingFacadeConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{HandlerTrace.Step(1)}\")) == \"step,RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => ThrowingFacadeConsumers.Pick(Enumerable.Distinct((dynamic)new[] { new LeftOnly() }), $\"{HandlerTrace.Step(1)}\")) == \"step,consumer1,InvalidOperationException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The same calls with the object results written in the index.
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick((IEnumerable<object>)Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{HandlerTrace.Step(1)}\")) == \"handler,step,consumer3\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick((IEnumerable<object>)Enumerable.Distinct((dynamic)new TracedSource<Both>()), $\"{HandlerTrace.Step(1)}\")) == \"source,handler,step,consumer3\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => FacadeConsumers.Pick((IEnumerable<object>)Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{HandlerTrace.Step(1) / HandlerTrace.Step(0)}\")) == \"handler,step,step,DivideByZeroException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => ThrowingFacadeConsumers.Pick((IEnumerable<object>)Enumerable.Distinct((dynamic)new[] { new Both() }), $\"{HandlerTrace.Step(1)}\")) == \"handler,step,consumer3,InvalidOperationException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The same calls with the sources themselves, which no result is selected for.
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => FacadeConsumers.Pick((dynamic)new[] { new Both() }, $\"{HandlerTrace.Step(1)}\")) == \"step,RuntimeBinderException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick((dynamic)new[] { new LeftOnly() }, $\"{HandlerTrace.Step(1)}\")) == \"step,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick((dynamic)new[] { new RightOnly() }, $\"{HandlerTrace.Step(1)}\")) == \"step,consumer2\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => HandlerTrace.Run(() => FacadeConsumers.Pick((dynamic)new TracedSource<LeftOnly>(), $\"{HandlerTrace.Step(1)}\")) == \"source,step,consumer1\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => FacadeConsumers.Pick((dynamic)new[] { new LeftOnly() }, $\"{HandlerTrace.Step(1) / HandlerTrace.Step(0)}\")) == \"step,step,DivideByZeroException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => FacadeTrace.Run(() => ThrowingFacadeConsumers.Pick((dynamic)new[] { new LeftOnly() }, $\"{HandlerTrace.Step(1)}\")) == \"step,consumer1,InvalidOperationException\") == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerConvertsArgumentsAfterTheResultsForTheSelectedView(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string>
                {
                    ["AmbiguousConsumers.cs"] = AmbiguousConsumerSources, ["BetterConsumers.cs"] = BetterConsumerSources, ["HandlerConsumers.cs"] = HandlerConsumerSources
                });
        }

        // Consumers of the element types of AmbiguousConsumerSources whose other arguments are target-typed expressions; the methods
        // and the handler constructor are internal, so their signatures are kept as declared.
        private const string HandlerConsumerSources = """
            using System;
            using System.Collections.Generic;
            [System.Runtime.CompilerServices.InterpolatedStringHandler]
            internal struct TextHandler
            {
                internal TextHandler(int literalLength, int formattedCount) => HandlerTrace.Add("handler");
                internal void AppendLiteral(string value) { }
                internal void AppendFormatted(int value) { }
                public static implicit operator TextHandler(string value)
                {
                    HandlerTrace.Add("implicit");
                    return new TextHandler(0, 0);
                }
            }
            public static class HandlerTrace
            {
                private static readonly List<string> Trace = new List<string>();
                internal static string Text = "1";
                internal static string Run(Func<int> consume)
                {
                    Trace.Clear();
                    consume();
                    return string.Join(",", Trace);
                }
                internal static int Step(int value)
                {
                    Trace.Add("step");
                    return value;
                }
                internal static void Add(string name) => Trace.Add(name);
                internal static int Consume(int overload)
                {
                    Trace.Add("consumer" + overload);
                    return overload;
                }
            }
            public static class HandlerConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, TextHandler text) => HandlerTrace.Consume(1);
                internal static int Pick(IEnumerable<IRight> values, string text) => HandlerTrace.Consume(2);
                internal static int Pick(IEnumerable<object> values, object text) => HandlerTrace.Consume(3);
            }
            public static class LeadingHandlerConsumers
            {
                internal static int Pick(TextHandler text, IEnumerable<ILeft> values) => 1;
                internal static int Pick(string text, IEnumerable<IRight> values) => 2;
                internal static int Pick(object text, IEnumerable<object> values) => 3;
            }
            public static class AmbiguousHandlerConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, TextHandler text) => 1;
                internal static int Pick(IEnumerable<IRight> values, TextHandler text) => 2;
                internal static int Pick(IEnumerable<object> values, object text) => 3;
            }
            public static class TupleConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, (long, long) pair) => 1;
                internal static int Pick(IEnumerable<IRight> values, (long, long) pair) => 2;
                internal static int Pick(IEnumerable<object> values, object pair) => 3;
                internal static int Pick(IEnumerable<ILeft> values, (byte, int) pair) => 4;
            }
            public static class ArmDropConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, long kind) => 1;
                internal static int Pick(IEnumerable<object> values, int kind) => 2;
            }
            public static class NullableTupleConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, (long, long)? pair) => 1;
                internal static int Pick(IEnumerable<IRight> values, (long, long)? pair) => 2;
                internal static int Pick(IEnumerable<object> values, object pair) => 3;
            }
            public static class NullableNumberConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, long? number) => 1;
                internal static int Pick(IEnumerable<IRight> values, long? number) => 2;
                internal static int Pick(IEnumerable<object> values, object number) => 3;
            }
            public static class KindConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, byte kind) => HandlerTrace.Consume(1);
                internal static int Pick(IEnumerable<IRight> values, int kind) => HandlerTrace.Consume(2);
                internal static int Pick(IEnumerable<object> values, object kind) => HandlerTrace.Consume(3);
            }
            public readonly struct LongPair
            {
                public static implicit operator (long, long)(LongPair pair) => (1, 2);
            }
            public static class FacadeConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, string text) => HandlerTrace.Consume(1);
                internal static int Pick(IEnumerable<IRight> values, string text) => HandlerTrace.Consume(2);
                internal static int Pick(IEnumerable<object> values, TextHandler text) => HandlerTrace.Consume(3);
                internal static int Pick(IEnumerable<object> values, string text) => HandlerTrace.Consume(4);
            }
            public static class ThrowingFacadeConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, string text) => HandlerTrace.Consume(1) + Throw();
                internal static int Pick(IEnumerable<IRight> values, string text) => HandlerTrace.Consume(2) + Throw();
                internal static int Pick(IEnumerable<object> values, TextHandler text) => HandlerTrace.Consume(3) + Throw();
                internal static int Pick(IEnumerable<object> values, string text) => HandlerTrace.Consume(4) + Throw();
                private static int Throw() => throw new InvalidOperationException();
            }
            public sealed class TracedSource<T> : IEnumerable<T> where T : new()
            {
                public TracedSource() => HandlerTrace.Add("source");
                public IEnumerator<T> GetEnumerator()
                {
                    HandlerTrace.Add("enumerate");
                    return ((IEnumerable<T>)new[] { new T() }).GetEnumerator();
                }
                System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
            }
            public static class FacadeTrace
            {
                internal static string Run(Func<int> consume) => HandlerTrace.Run(() =>
                {
                    try
                    {
                        return consume();
                    }
                    catch (Exception e)
                    {
                        HandlerTrace.Add(e.GetType().Name);
                        return 0;
                    }
                });
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        // A result of numbers keeps its element type for a generic, object or nongeneric parameter, and a parameter that takes only
        // objects, also as a params array beside overloads of unrelated element types, gets the numbers as objects.
        [RavenData("GenericProbe", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("GenericOnlyProbe", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("ObjectProbe", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("UntypedProbe", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("OnlyObjectsProbe", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("ParamsObjectsProbe", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerKeepsNumbersOrTakesThemAsObjects(Options options, string consumer)
        {
            var expression = $"Enumerable.Count(item.Tags, tag => {consumer}.Pick(Enumerable.Distinct((dynamic)new[] {{ 1L, 1L, 2L }}))) == 2";
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["ElementTypeConsumers.cs"] = ElementTypeConsumerSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // A params array of object sequences takes the result after the arguments written before it, and a result of strings keeps
        // its element type inside the array.
        [RavenData("ParamsPrefixProbe.Pick(\"prefix\", Enumerable.Distinct((dynamic)new[] { 1L, 1L, 2L }))", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("ParamsReferenceProbe.Pick(Enumerable.Distinct((dynamic)new[] { \"a\", \"a\", \"b\" }))", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctConsumerTakesResultInParamsArrayOfObjectSequences(Options options, string predicate)
        {
            var expression = $"Enumerable.Count(item.Tags, tag => {predicate}) == 2";
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["ElementTypeConsumers.cs"] = ElementTypeConsumerSources });
        }

        // The methods are internal, so their signatures are kept as declared.
        private const string ElementTypeConsumerSources = """
            using System;
            using System.Collections;
            using System.Collections.Generic;
            public static class GenericProbe
            {
                internal static bool Pick<T>(IEnumerable<T> values) => typeof(T) == typeof(long);
                internal static bool Pick(IEnumerable<string> values) => false;
            }
            public static class GenericOnlyProbe
            {
                internal static bool Pick<T>(IEnumerable<T> values) => typeof(T) == typeof(long);
            }
            public static class ObjectProbe
            {
                internal static bool Pick(IEnumerable<int> values) => false;
                internal static bool Pick(object values) => values is IEnumerable<long>;
            }
            public static class UntypedProbe
            {
                internal static bool Pick(IEnumerable<int> values) => false;
                internal static bool Pick(IEnumerable values) => values is IEnumerable<long>;
            }
            public static class OnlyObjectsProbe
            {
                internal static bool Pick(IEnumerable<object> values) => System.Linq.Enumerable.Count(values) == 2;
            }
            public interface IUnrelatedLeft
            {
            }
            public interface IUnrelatedRight
            {
            }
            public static class ParamsObjectsProbe
            {
                internal static bool Pick(IEnumerable<IUnrelatedLeft> values) => false;
                internal static bool Pick(IEnumerable<IUnrelatedRight> values) => false;
                internal static bool Pick(params IEnumerable<object>[] values) => values.Length == 1 && System.Linq.Enumerable.Count(values[0]) == 2;
            }
            public static class ParamsPrefixProbe
            {
                internal static bool Pick(string prefix, IEnumerable<IUnrelatedLeft> values) => false;
                internal static bool Pick(string prefix, IEnumerable<IUnrelatedRight> values) => false;
                internal static bool Pick(string prefix, params IEnumerable<object>[] values) => prefix == "prefix" && values.Length == 1 && System.Linq.Enumerable.Count(values[0]) == 2;
            }
            public static class ParamsReferenceProbe
            {
                internal static bool Pick(IEnumerable<IUnrelatedLeft> values) => false;
                internal static bool Pick(IEnumerable<IUnrelatedRight> values) => false;
                internal static bool Pick(params IEnumerable<object>[] values) => values.Length == 1 && values[0] is IEnumerable<string> && System.Linq.Enumerable.Count(values[0]) == 2;
            }
            """;

        // The methods are internal, so their signatures are kept as declared.
        private const string AmbiguousConsumerSources = """
            using System;
            using System.Collections.Generic;
            public interface ILeft { }
            public interface IRight { }
            public sealed class Both : ILeft, IRight { }
            public sealed class LeftOnly : ILeft { }
            public sealed class RightOnly : IRight { }
            public static class AmbiguousConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values) => 1;
                internal static int Pick(IEnumerable<IRight> values) => 2;
                internal static int Pick(IEnumerable<object> values) => 3;
            }
            public sealed class AmbiguousHolder
            {
                public readonly int Overload;
                internal AmbiguousHolder(IEnumerable<ILeft> values) => Overload = 1;
                internal AmbiguousHolder(IEnumerable<IRight> values) => Overload = 2;
                internal AmbiguousHolder(IEnumerable<object> values) => Overload = 3;
            }
            public sealed class TracedConsumer
            {
                private static readonly List<string> Trace = new List<string>();
                internal static string Run(Func<int> consume)
                {
                    Trace.Clear();
                    consume();
                    return string.Join(",", Trace);
                }
                internal static TracedConsumer Create()
                {
                    Trace.Add("receiver");
                    return new TracedConsumer();
                }
                internal static string Step(string name)
                {
                    Trace.Add(name);
                    return name;
                }
                internal static IEnumerable<Both> BothValues() => Values(new Both());
                internal static IEnumerable<LeftOnly> LeftOnlyValues() => Values(new LeftOnly());
                private static IEnumerable<T> Values<T>(T value)
                {
                    Trace.Add("source");
                    return Enumerate(value);
                }
                private static IEnumerable<T> Enumerate<T>(T value)
                {
                    Trace.Add("enumerated");
                    yield return value;
                }
                internal int Pick(string before, IEnumerable<ILeft> values, string after) => Consume(1);
                internal int Pick(string before, IEnumerable<IRight> values, string after) => Consume(2);
                internal int Pick(string before, IEnumerable<object> values, string after) => Consume(3);
                internal static string RunAction(Action consume)
                {
                    Trace.Clear();
                    consume();
                    return string.Join(",", Trace);
                }
                internal void Record(IEnumerable<ILeft> values) => Consume(1);
                internal void Record(IEnumerable<IRight> values) => Consume(2);
                internal void Record(IEnumerable<object> values) => Consume(3);
                private static int Consume(int overload)
                {
                    Trace.Add("consumer" + overload);
                    return overload;
                }
            }
            """;

        // Consumers of the element types of AmbiguousConsumerSources; the methods are internal, so their signatures are kept as declared.
        private const string ConsumerPlanSources = """
            using System;
            using System.Collections.Generic;
            public static class PairConsumers
            {
                internal static int Pick(IEnumerable<ILeft> first, IEnumerable<ILeft> second) => 1;
                internal static int Pick(IEnumerable<IRight> first, IEnumerable<IRight> second) => 2;
                internal static int Pick(IEnumerable<object> first, IEnumerable<object> second) => 3;
            }
            public sealed class PairHolder
            {
                public readonly int Overload;
                internal PairHolder(IEnumerable<ILeft> first, IEnumerable<ILeft> second) => Overload = 1;
                internal PairHolder(IEnumerable<IRight> first, IEnumerable<IRight> second) => Overload = 2;
                internal PairHolder(IEnumerable<object> first, IEnumerable<object> second) => Overload = 3;
            }
            public static class DiscriminatedConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values, int kind) => 1;
                internal static int Pick(IEnumerable<IRight> values, string kind) => 2;
                internal static int Pick(IEnumerable<object> values, object kind) => 3;
            }
            public static class VoidMemberConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values) => 1;
                internal static int Pick(IEnumerable<IRight> values) => 2;
                internal static int Pick(IEnumerable<object> values) => 3;
                internal static void Pick() { }
            }
            public static class GenericMemberConsumers
            {
                internal static int Pick(IEnumerable<ILeft> values) => 1;
                internal static int Pick(IEnumerable<IRight> values) => 2;
                internal static int Pick(IEnumerable<object> values) => 3;
                internal static int Pick<T>() => 0;
            }
            public interface __distinctCallX { }
            public sealed class PrefixLeft : __distinctCallX { }
            public static class PrefixConsumers
            {
                internal static int Pick(IEnumerable<__distinctCallX> values) => 1;
                internal static int Pick(IEnumerable<IRight> values) => 2;
                internal static int Pick(IEnumerable<object> values) => 3;
            }
            public sealed class TracedPairs
            {
                private static readonly List<string> Trace = new List<string>();
                internal static string Run(Func<int> consume)
                {
                    Trace.Clear();
                    consume();
                    return string.Join(",", Trace);
                }
                internal static TracedPairs Create()
                {
                    Trace.Add("receiver");
                    return new TracedPairs();
                }
                internal static string Step(string name)
                {
                    Trace.Add(name);
                    return name;
                }
                internal static IEqualityComparer<Both> Comparer()
                {
                    Trace.Add("comparer");
                    return EqualityComparer<Both>.Default;
                }
                internal static IEnumerable<Both> BothValues() => Values(new Both());
                internal static IEnumerable<LeftOnly> LeftOnlyValues() => Values(new LeftOnly());
                private static IEnumerable<T> Values<T>(T value)
                {
                    Trace.Add("source");
                    return Enumerate(value);
                }
                private static IEnumerable<T> Enumerate<T>(T value)
                {
                    Trace.Add("enumerated");
                    yield return value;
                }
                internal int Pick(string before, IEnumerable<ILeft> first, string middle, IEnumerable<ILeft> second, string after) => Consume(1);
                internal int Pick(string before, IEnumerable<IRight> first, string middle, IEnumerable<IRight> second, string after) => Consume(2);
                internal int Pick(string before, IEnumerable<object> first, string middle, IEnumerable<object> second, string after) => Consume(3);
                private static int Consume(int overload)
                {
                    Trace.Add("consumer" + overload);
                    return overload;
                }
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        // A type of the index sources with the name of the framework comparer interface does not change how the comparer of
        // a nested Distinct call is recognized.
        public void NestedDistinctRecognizesFrameworkComparer(Options options, bool comparerInterfaceSource)
        {
            AssertPrimitiveArrayExpression(options,
                "Enumerable.Count(item.Tags, tag => Enumerable.Distinct(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 }), EqualityComparer<int>.Default).Count() == 2) == 2",
                new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                comparerInterfaceSource ? new Dictionary<string, string> { ["Comparer.cs"] = ComparerInterfaceSource } : null);
        }

        private const string ComparerInterfaceSource = """
            namespace System.Collections.Generic
            {
                public interface IEqualityComparer<T> { }
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(tag) > 3) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Distinct(tag)) == 4) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Range(-2, 3).Select(Math.Abs).Sum() == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Contains(Enumerable.Range(0, 3).Select(x => (long)x), 2L)) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Select(Enumerable.Range(0, 3), Convert.ToString).Contains(\"2\")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Where((dynamic)Enumerable.First(Enumerable.Select(new object[] { 1 }, x => Enumerable.Range(1, 2))), (Func<dynamic, bool>)(x => x > 1))) == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void RewrittenCallsInsideCountExecuteInReduce(Options options, string expression)
        {
            AssertReduceExpression(options, expression);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // The generated Range source choice of a reduce rewrites the lambdas of the call written in the index,
        // and a type of the index sources with the name or the full name of the helper type does not capture it.
        [RavenData(false, "Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).SelectMany(x => new object[] { \"2024-01-01T00:00:00.0000000Z\" }).First() is DateTime) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).SelectMany(x => new object[] { \"2024-01-01T00:00:00.0000000Z\" }).First() is DateTime) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).Select((Func<dynamic, dynamic>)(x => x)).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(item.Tags, tag => Enumerable.Range(0, 1).Select((Func<dynamic, dynamic>)(x => x)).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void GeneratedSourceChoicesInReduceIgnoreIndexTypesWithHelperNames(Options options, bool fullName, string expression)
        {
            AssertReduceExpression(options, expression, new Dictionary<string, string> { ["HelperNames.cs"] = fullName ? HelperFullNameSources : HelperIndexNamespaceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // The helpers that replace Enumerable calls are not captured by a type of the index sources with the name or the full name
        // of the helper type, so calls with reference elements keep working as Enumerable calls did.
        [RavenData(false, "Enumerable.Count(new[] { \"a\", \"b\" }) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(new[] { \"a\", \"b\" }) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Contains(new[] { \"a\" }, \"a\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Contains(new[] { \"a\" }, \"a\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.SequenceEqual(new[] { \"a\" }, new[] { \"a\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.SequenceEqual(new[] { \"a\" }, new[] { \"a\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Concat(new[] { \"a\" }, new[] { \"b\" }).Last() == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Concat(new[] { \"a\" }, new[] { \"b\" }).Last() == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.ToArray(new[] { \"a\", \"b\" }).Length == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.ToArray(new[] { \"a\", \"b\" }).Length == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.ToList(new[] { \"a\", \"b\" }).Count == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.ToList(new[] { \"a\", \"b\" }).Count == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // Distinct gets the helpers only inside rewritten calls; at the top level it stays an Enumerable call.
        [RavenData(false, "Enumerable.Count(item.Tags, tag => Enumerable.Distinct(new[] { \"a\", \"a\" }).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(item.Tags, tag => Enumerable.Distinct(new[] { \"a\", \"a\" }).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Count(item.Tags, tag => Enumerable.Distinct(comparer: StringComparer.Ordinal, source: new[] { \"a\", \"a\" }).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(item.Tags, tag => Enumerable.Distinct(comparer: StringComparer.Ordinal, source: new[] { \"a\", \"a\" }).Count() == 1) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(item.Tags, tag => Enumerable.Count<int>(Enumerable.Distinct((dynamic)new[] { 1, 1, 2 })) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "Enumerable.Distinct(new[] { \"a\", \"a\" }).Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Distinct(new[] { \"a\", \"a\" }).Count() == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        // Ordering of string arrays with the default comparer, with a dynamic key selector and with a comparer.
        [RavenData(false, "new string[] { \"b\", \"a\" }.OrderBy(x => x).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "new string[] { \"b\", \"a\" }.OrderBy(x => x).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, "new string[] { \"B\", \"a\" }.OrderBy(x => x, StringComparer.InvariantCulture).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "new string[] { \"B\", \"a\" }.OrderBy(x => x, StringComparer.InvariantCulture).First() == \"a\"", SearchEngineMode = RavenSearchEngineMode.All)]
        public void HelperCallsIgnoreIndexTypesWithHelperNames(Options options, bool fullName, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["HelperNames.cs"] = fullName ? HelperFullNameSources : HelperIndexNamespaceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, "Enumerable.Count(new[] { \"a\", \"b\" }) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, "Enumerable.Count(new[] { \"a\", \"b\" }) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void HelperCallsInReduceIgnoreIndexTypesWithHelperNames(Options options, bool fullName, string expression)
        {
            AssertReduceExpression(options, expression, new Dictionary<string, string> { ["HelperNames.cs"] = fullName ? HelperFullNameSources : HelperIndexNamespaceSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        // The helper references of each index use a name that its code does not use, so a type of the index sources imported
        // through its namespace keeps its name, also when it is the name the helper references get in another index.
        public void HelperCallsKeepNamesOfIndexTypes(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } });
                session.SaveChanges();
            }

            var results = new List<string>();
            foreach (var typeName in new[] { "ravenServer_1", "ravenServer" })
            {
                var indexName = "HelperNames/" + typeName;
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = indexName,
                    Maps = { "from item in docs.PrimitiveArrayItems select new { Found = " + typeName + ".Eleven() == 11 && Enumerable.Count(new[] { \"a\", \"b\" }) == 2 }" },
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["IndexNames.cs"] = "namespace IndexNames { public static class " + typeName + " { public static int Eleven() => 11; } }"
                    },
                    Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
                }));

                Indexes.WaitForIndexing(store, allowErrors: true);
                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
                using var session = store.OpenSession();
                var found = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single().Found;
                results.Add(typeName + ":" + found + string.Concat(errors.Select(x => Environment.NewLine + x.Error)));
            }

            Assert.Equal(new[] { "ravenServer_1:True", "ravenServer:True" }, results);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        // A type named Enumerable declared in the namespace of the index hides System.Linq.Enumerable for the index code, so a call
        // written as Enumerable.Contains binds to the method of the index sources and keeps its result.
        public void ContainsOfEnumerableTypeDeclaredInIndexNamespaceKeepsUserMethod(Options options)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/IndexNamespaceEnumerableContains";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = Enumerable.Contains(new[] { \"a\" }, \"a\") }" },
                AdditionalSources = new Dictionary<string, string>
                {
                    ["IndexNamespaceEnumerable.cs"] = """
                        namespace Raven.Server.Documents.Indexes.Static.Generated
                        {
                            public static class Enumerable
                            {
                                public static bool Contains(object source, object value) => false;
                            }
                        }
                        """
                },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "a" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single();
                Assert.False(result.Found, "Enumerable.Contains must bind to the Enumerable type of the index sources, which returns false.");
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        // The index may call the object delegate source helper itself, without additional sources or assemblies. Such a call stays an
        // ordinary call of the identity helper and is not lowered like a generated marker, so it compiles without the extern alias
        // that only the generated markers use.
        public void ExplicitObjectDelegateSourceCompilesWithoutAdditionalSources(Options options)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/ExplicitObjectDelegateSource";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Values = Enumerable.Where(DynamicEnumerable.ObjectDelegateSource(item.Tags), tag => tag != null) }" },
                Fields = { ["Values"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<StoredTagsResult>($"from index '{indexName}' select Values").Single();
                Assert.Equal(new[] { "alpha", "b" }, result.Values);
            }
        }

        private sealed class StoredTagsResult
        {
            public string[] Values { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Count(Enumerable.Distinct((dynamic)" + InternalTupleValues + ")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Distinct(comparer: (dynamic)null, source: (dynamic)" + InternalTupleValues + ")) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Distinct(Enumerable.Distinct((dynamic)" + InternalTupleValues + "))) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Distinct(Enumerable.Distinct((dynamic)" + InternalTupleValues + "), EqualityComparer<object>.Default)) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Distinct((IEnumerable<object>)Enumerable.Distinct((dynamic)" + InternalTupleValues + "))) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleProbe.Tally(Enumerable.Distinct((dynamic)" + InternalTupleValues + ")) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => TupleFallbackProbe.Tally(Enumerable.Distinct((dynamic)" + InternalTupleValues + "), value => 1) == 2) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(Enumerable.Distinct(Enumerable.Distinct((dynamic)new[] { \"a\", \"a\", \"b\" }))) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        // The PR already supports comparer-first arguments over an iterator; keep that behavior while moving the call site.
        [RavenData("Enumerable.Count(Enumerable.Distinct(comparer: (dynamic)null, source: Enumerable.Distinct((dynamic)new[] { \"a\", \"a\", \"b\" }))) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctKeepsInternalAdditionalSourceEquality(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["InternalTuples.cs"] = """
                    using System;
                    using System.Collections.Generic;
                    internal sealed class LocalTuple : Tuple<int>, IEquatable<LocalTuple>
                    {
                        public LocalTuple(int value) : base(value) { }
                        bool IEquatable<LocalTuple>.Equals(LocalTuple other) => object.ReferenceEquals(this, other);
                    }
                    public static class TupleProbe
                    {
                        internal static int Tally(IEnumerable<LocalTuple> values) => System.Linq.Enumerable.Count(values);
                        internal static int Tally(IEnumerable<object> values) => -1;
                    }
                    public static class TupleFallbackProbe
                    {
                        internal static int Tally(IEnumerable<LocalTuple> values, Func<LocalTuple, int> selector) => -1;
                        internal static int Tally(IEnumerable<object> values, Func<object, int> selector) => System.Linq.Enumerable.Sum(values, selector);
                    }
                    """ });
        }

        private const string InternalTupleValues = "new[] { new LocalTuple(1), new LocalTuple(1) }";

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistinctGenericConsumerKeepsExplicitTypeArgument(Options options)
        {
            AssertPrimitiveArrayExpression(options,
                "Enumerable.Count(item.Tags, tag => !GenericOnlyProbe.Pick<object>(Enumerable.Distinct((dynamic)new[] { \"a\", \"a\", \"b\" }))) == 2",
                new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } },
                new Dictionary<string, string> { ["ElementTypeConsumers.cs"] = ElementTypeConsumerSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        public void EnumerableVariableKeepsItsCustomContains(Options options, bool shadowsEnumerable)
        {
            string variable = shadowsEnumerable ? "Enumerable" : "Probe";
            string expression = variable + ".Contains(new[] { \"A\" }, \"a\")";
            if (shadowsEnumerable == false)
                expression += " && Enumerable.Contains(item.Values, 1)";
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a" }, Values = new[] { 1L } },
                new Dictionary<string, string>
                {
                    ["CustomProbe.cs"] = """
                        public class CustomProbe
                        {
                            internal bool Contains(string[] source, string value) =>
                                System.Linq.Enumerable.Contains(source, value, System.StringComparer.OrdinalIgnoreCase);
                        }
                        """
                }, lets: "let " + variable + " = new CustomProbe()");
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NestedUserDefinedCountKeepsItsArgumentType(Options options)
        {
            // Deliberate name shadowing through the supported AdditionalSources API.
            AssertPrimitiveArrayExpression(options, "Enumerable.Count(item.Tags, tag => Enumerable.Count(\"abc\") == 3) == 2",
                new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } }, new Dictionary<string, string>
                {
                    ["UserEnumerable.cs"] = """
                        namespace Raven.Server.Documents.Indexes.Static.Generated
                        {
                            public static class Enumerable
                            {
                                internal static int Count(string value) => value.Length;
                                internal static int Count(System.Collections.Generic.IEnumerable<object> source, System.Func<object, bool> predicate) =>
                                    System.Linq.Enumerable.Count(source, predicate);
                            }
                        }
                        """
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void StringArrayOrderingKeepsUserExtensionBinding(Options options, bool alsoUsesLinqContains)
        {
            string expression = "new string[] { \"b\", \"a\" }.OrderBy(value => value).First() == \"b\"";
            if (alsoUsesLinqContains)
                expression += " && Enumerable.Contains(item.Values, 1)";
            // A deliberately diagnostic extension distinguishes user binding from the built-in sort.
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Values = new[] { 1L } },
                new Dictionary<string, string>
                {
                    ["StringArrayOrder.cs"] = """
                        namespace Raven.Server.Documents.Indexes.Static.Generated
                        {
                            public static class StringArrayOrder
                            {
                                internal static string[] OrderBy(this string[] source, System.Func<object, object> keySelector) => source;
                            }
                        }
                        """
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("lambda", true, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("user-int", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("mixed", false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("user-struct", false, SearchEngineMode = RavenSearchEngineMode.All)]
        public void StringArrayOrderingIgnoresInapplicableExtensions(Options options, string calls, bool optimized)
        {
            const string lambda = "new string[] { \"b\", \"a\" }.OrderBy(value => value).First() == \"a\"";
            const string userCall = "new string[] { \"b\", \"a\" }.OrderBy(1).First() == \"b\"";
            string expression = calls switch
            {
                "lambda" => lambda,
                "user-int" => userCall,
                "user-struct" => "new string[] { \"b\", \"a\" }.OrderBy(value => value).First() == \"b\"",
                _ => userCall + " && " + lambda
            };
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/InapplicableOrdering";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { Found = " + expression + " }" },
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } },
                AdditionalSources = new Dictionary<string, string>
                {
                    ["StringArrayOrder.cs"] = calls == "user-struct" ? """
                        namespace Raven.Server.Documents.Indexes.Static.Generated
                        {
                            public readonly struct Selector
                            {
                                public static implicit operator Selector(System.Func<object, object> value) => default;
                            }
                            public static class StringArrayOrder
                            {
                                internal static string[] OrderBy(this string[] source, Selector selector) => source;
                            }
                        }
                        """ : """
                        namespace PerfUnused
                        {
                            public static class StringArrayOrder
                            {
                                internal static string[] OrderBy(this string[] source, int mode) => source;
                            }
                        }
                        """
                }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem());
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors);
            using (var session = store.OpenSession())
                Assert.True(session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single().Found);

            // Both routes return the right result; the dispatch assertion protects the allocation-sensitive path.
            var index = (MapIndex)Databases.GetDocumentDatabaseInstanceFor(store).GetAwaiter().GetResult().IndexStore.GetIndex(indexName);
            Assert.Equal(optimized, index._compiled.Source.Contains("OrderByWithDefaultComparer"));
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Contains(d.Tags, \"x\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(d.Tags, new[] { \"x\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        public void StoredStringSequenceOperandFailsOnlyItsDocumentInsteadOfStoringFalse(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "StringOperands/StaticSequenceCall";
            const string stringId = "StringOperandItems/string-tags";
            string map = "from d in docs.StringOperandItems select new { d.Name, Has = " + expression + " }";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["Name"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Has"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new StringOperandItem { Name = "string-tags", Tags = "x" }, stringId);
                session.Store(new StringOperandItem { Name = "array-tags", Tags = Array.Empty<string>() });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            string snapshot = WaitForOperandSnapshot(store, indexName, $"attempts=2 errors=1 entries=1 failed=[{stringId}:RuntimeBinderException]");
            List<ScalarOperandResult> rows;
            using (var session = store.OpenSession())
                rows = session.Advanced.RawQuery<ScalarOperandResult>($"from index '{indexName}' select Name, Has").ToList();
            string observed = snapshot + $" rows=[{string.Join(", ", rows.OrderBy(row => row.Name).Select(row => row.Name + ":" + row.Has))}]";
            Output.WriteLine(expression + " => " + observed);
            Assert.Equal($"attempts=2 errors=1 entries=1 failed=[{stringId}:RuntimeBinderException] rows=[array-tags:False]", observed);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("item.Group.Contains(\"x\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Group, 'x')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(item.Group, \"x-mas\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(item.Tags, \"x-mas\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.Contains(item.Tags, \"x\")", SearchEngineMode = RavenSearchEngineMode.All)]
        public void StringOperandsKeepCharacterAndInstanceSemantics(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression,
                new PrimitiveArrayItem { Group = "x-mas", Tags = new[] { "x-mas", "hanuka" } });
        }

        private sealed class StringOperandItem
        {
            public string Name { get; set; }
            public object Tags { get; set; }
        }

        private string WaitForOperandSnapshot(DocumentStore store, string indexName, string expected)
        {
            return WaitForValue(() =>
            {
                IndexStats stats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexName));
                IndexingError[] errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
                return $"attempts={stats.MapAttempts} errors={stats.MapErrors} entries={stats.EntriesCount} " +
                       $"failed=[{string.Join(", ", errors.OrderBy(error => error.Document).Select(error => error.Document + ":" + (error.Error.Contains("RuntimeBinderException") ? "RuntimeBinderException" : error.Error)))}]";
            }, expected, timeout: 15_000, interval: 100);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("Enumerable.Contains(d.Age, 5)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Contains(d.Age, 5, EqualityComparer<object>.Default)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(d.Tags, d.Age)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.SequenceEqual(d.Age, d.Tags)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ScalarSequenceOperandFailsOnlyItsDocumentInsteadOfStoringFalse(Options options, string expression)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "ScalarOperands/StaticSequenceCall";
            const string scalarAgeId = "ScalarOperandItems/scalar-age";
            const string nullAgeId = "ScalarOperandItems/null-age";
            string map = "from d in docs.ScalarOperandItems select new { d.Name, Has = " + expression + " }";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["Name"] = new IndexFieldOptions { Storage = FieldStorage.Yes }, ["Has"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            Assert.Equal(map, Assert.Single(store.Maintenance.Send(new GetIndexesOperation(0, 10)).Single().Maps));
            using (var session = store.OpenSession())
            {
                session.Store(new ScalarOperandItem { Name = "scalar-age", Age = 5L, Tags = new[] { "a" } }, scalarAgeId);
                session.Store(new ScalarOperandItem { Name = "null-age", Age = null, Tags = new[] { "a" } }, nullAgeId);
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            string snapshot = WaitForOperandSnapshot(store, indexName, $"attempts=2 errors=1 entries=1 failed=[{scalarAgeId}:RuntimeBinderException]");
            List<ScalarOperandResult> rows;
            using (var session = store.OpenSession())
                rows = session.Advanced.RawQuery<ScalarOperandResult>($"from index '{indexName}' select Name, Has").ToList();

            string observed = snapshot + $" rows=[{string.Join(", ", rows.OrderBy(x => x.Name).Select(x => x.Name + ":" + x.Has))}]";
            Output.WriteLine(expression + " => " + observed);
            Assert.Equal($"attempts=2 errors=1 entries=1 failed=[{scalarAgeId}:RuntimeBinderException] rows=[null-age:False]", observed);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.Contains((dynamic)null, null))) == \"RuntimeBinderException\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("ErrorProbe.ExceptionType((Func<bool>)(() => Enumerable.SequenceEqual((dynamic)null, (dynamic)null))) == \"RuntimeBinderException\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.Contains(item.MissingTags, \"a\", StringComparer.Ordinal)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(item.MissingTags, new[] { 1L })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(new[] { 1L }, item.MissingTags)", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ScalarSequenceOperandGuardKeepsNullAndMissingBehavior(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a" } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        private sealed class ScalarOperandItem
        {
            public string Name { get; set; }

            public long? Age { get; set; }

            public string[] Tags { get; set; }
        }

        private sealed class ScalarOperandResult
        {
            public string Name { get; set; }

            public bool Has { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // Every static call the index rewrites binds to the Enumerable type of the index sources when that type hides System.Linq.Enumerable,
        // in the map and in the reduce. The static ordering of a literal array keeps the user method too.
        [RavenData("!Enumerable.Contains(new[] { \"a\" }, \"a\")", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.Contains(new[] { \"a\" }, \"a\", null)", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("!Enumerable.SequenceEqual(new[] { \"a\" }, new[] { \"a\" })", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("\"index\".Equals(Enumerable.Concat(new[] { \"a\" }, new[] { \"b\" }))", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("\"index\".Equals(Enumerable.ToArray(new[] { \"a\" }))", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("\"index\".Equals(Enumerable.ToList(new[] { \"a\" }))", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("\"index\".Equals(Enumerable.Count(new[] { \"a\" }))", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("\"index\".Equals(Enumerable.OrderBy(new[] { \"b\", \"a\" }, x => x).First())", SearchEngineMode = RavenSearchEngineMode.All)]
        public void StaticCallsOfEnumerableTypeDeclaredInIndexNamespaceKeepUserMethods(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "a" } },
                new Dictionary<string, string> { ["IndexNamespaceEnumerable.cs"] = IndexNamespaceEnumerableSources });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ContainsOfEnumerableTypeDeclaredInIndexNamespaceKeepsUserMethodInReduce(Options options)
        {
            AssertReduceExpression(options, "!Enumerable.Contains(new[] { \"a\" }, \"a\")",
                new Dictionary<string, string> { ["IndexNamespaceEnumerable.cs"] = IndexNamespaceEnumerableSources });
        }

        private const string IndexNamespaceEnumerableSources = """
            using System;
            using System.Collections.Generic;

            namespace Raven.Server.Documents.Indexes.Static.Generated
            {
                public static class Enumerable
                {
                    public static bool Contains(object source, object value) => false;
                    public static bool Contains(object source, object value, object comparer) => false;
                    public static bool SequenceEqual(object first, object second) => false;
                    public static string Concat(object first, object second) => "index";
                    public static string ToArray(object source) => "index";
                    public static string ToList(object source) => "index";
                    public static string Count(object source) => "index";

                    // A block body: the server rewrites the public methods of the index sources and does not accept an expression-bodied
                    // method with a sequence result.
                    public static IEnumerable<object> OrderBy(object source, Func<object, object> keySelector)
                    {
                        return new object[] { "index" };
                    }
                }
            }
            """;

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        // A nested type of the index sources named Enumerable does not hide System.Linq.Enumerable, so the rewritten calls keep the server's
        // handling of stored nulls.
        public void HelperCallsKeepServerBindingNextToNestedEnumerableType(Options options)
        {
            AssertPrimitiveArrayExpression(options, "Enumerable.Contains(item.Tags, null)", new PrimitiveArrayItem { Tags = new[] { "a", null, "b" } },
                new Dictionary<string, string>
                {
                    ["NestedEnumerable.cs"] = """
                        namespace IndexHelpers
                        {
                            public static class Holder
                            {
                                public static class Enumerable
                                {
                                    public static bool Contains(object source, object value) => false;
                                }
                            }
                        }
                        """
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        // A generated object delegate marker keeps its lowering next to a call of the helper written in the index.
        public void GeneratedObjectDelegateMarkerNextToExplicitHelperCallKeepsLowering(Options options)
        {
            AssertPrimitiveArrayExpression(options, "Enumerable.Count(Enumerable.Where(DynamicEnumerable.ObjectDelegateSource(item.Tags), x => x != null)) == 2",
                new PrimitiveArrayItem { Tags = new[] { "a", null, "b" } });
        }

        private void AssertReduceExpression(Options options, string expression, Dictionary<string, string> additionalSources = null)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/NestedCallsReduce";
            var definition = new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.PrimitiveArrayItems select new { item.Group, item.Tags, Found = false }" },
                Reduce = "from result in results group result by result.Group into grouped let item = grouped.First() select new { Group = grouped.Key, item.Tags, Found = " + expression + " }",
                Fields = { ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            };
            if (additionalSources != null)
                definition.AdditionalSources = additionalSources;
            store.Maintenance.Send(new PutIndexesOperation(definition));
            using (var session = store.OpenSession())
            {
                session.Store(new PrimitiveArrayItem { Group = "tags", Tags = new[] { "alpha", "b" } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), expression + Environment.NewLine + string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                var result = session.Advanced.RawQuery<ClrContainsResult>($"from index '{indexName}' select Found").Single();
                Assert.True(result.Found, expression);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        // Results of internal BCL iterators keep their public interfaces for dynamic binding.
        [RavenData("Enumerable.First(Enumerable.Where(item.Tags, tag => tag == \"b\")) == \"b\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.First(Enumerable.Reverse(tag)) == 'a') == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        // The calls are fully qualified so no rewrite applies: Count binds to the char elements of a dynamic Distinct result.
        [RavenData("Enumerable.Count(item.Tags, tag => System.Linq.Enumerable.Count(System.Linq.Enumerable.Distinct(tag)) == 4) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => System.Linq.Enumerable.Count(System.Linq.Enumerable.Distinct((IEnumerable<char>)tag)) == 4) == 1", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DynamicBindingOfIteratorResultsExecutes(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Tags = new[] { "alpha", "b" } });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualUsesTypedEnumeratorOfDerivedArray(Options options, bool valuesFirst)
        {
            using var store = GetDocumentStore(options);
            const string indexName = "PrimitiveArrays/TypedSequenceEqual";
            string expression = valuesFirst
                ? "Enumerable.SequenceEqual((IEnumerable<short>)values, new short[] { 5, 6 })"
                : "Enumerable.SequenceEqual(new short[] { 5, 6 }, (IEnumerable<short>)values)";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from item in docs.ContainsOriginItems let values = new TypedSequenceValues(item.Values) select new { item.Key, Found = " + expression + ", Trace = values.Trace.ToArray() }" },
                AdditionalSources = new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources },
                Fields =
                {
                    ["Key"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["Found"] = new IndexFieldOptions { Storage = FieldStorage.Yes },
                    ["Trace"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
                }
            }));
            using (var session = store.OpenSession())
            {
                session.Store(new ContainsOriginItem { Key = "equal", Values = new[] { 5L, 6L } });
                session.Store(new ContainsOriginItem { Key = "longer", Values = new[] { 5L, 6L, 7L } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store, allowErrors: true);
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).Single().Errors;
            Assert.False(errors.Any(), string.Join(Environment.NewLine, errors.Select(x => x.Error)));
            using (var session = store.OpenSession())
            {
                ContainsOriginResult[] rows = session.Advanced.RawQuery<ContainsOriginResult>($"from index '{indexName}' select Key, Found, Trace").ToList().OrderBy(row => row.Key).ToArray();
                Assert.Equal(new[] { ("equal", true), ("longer", false) }, rows.Select(row => (row.Key, row.Found)).ToArray());
                // Both argument orders read the derived array through its typed enumerator, which is disposed.
                Assert.Equal(new[]
                {
                    ("equal", "short.GetEnumerator|short.MoveNext:5|short.MoveNext:6|short.Dispose"),
                    ("longer", "short.GetEnumerator|short.MoveNext:5|short.MoveNext:6|short.MoveNext:7|short.Dispose")
                }, rows.Select(row => (row.Key, string.Join("|", row.Trace ?? Array.Empty<string>()))).ToArray());
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData("ErrorProbe.ParamName((Func<bool>)(() => Enumerable.SequenceEqual((IEnumerable<object>)item.Values, (IEnumerable<object>)null))) == \"second\"", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("ErrorProbe.ParamName((Func<bool>)(() => Enumerable.SequenceEqual((IEnumerable<object>)null, (IEnumerable<object>)item.Values))) == \"first\"", SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualOfStoredArrayReportsNullArgument(Options options, string expression)
        {
            AssertPrimitiveArrayExpression(options, expression, new PrimitiveArrayItem { Values = new[] { 1L } },
                new Dictionary<string, string> { ["NestedSequences.cs"] = NestedSequenceSources });
        }

        // Additional sources are rewritten: public methods get dynamic object/numeric parameters and returns, so
        // RangeSelectors.Box takes object elements, while internal methods, properties, constructors and explicit
        // interface implementations keep their signatures.
        private const string NestedSequenceSources = """
            using System;
            using System.Collections;
            using System.Collections.Generic;
            using System.Dynamic;
            using System.Linq;
            using Raven.Server.Documents.Indexes.Static;
            public static class RangeSelectors
            {
                public static object Box(int value)
                {
                    return value;
                }
                internal static int Twice(int value) => value * 2;
            }
            public static class OverloadedSelectors
            {
                internal static string Label(int value) => "int";
                internal static string Label(object value) => "object";
                internal static string LabelIndexed(int value, int index) => "int";
                internal static string LabelIndexed(object value, int index) => "object";
            }
            public sealed class IdentityTuple : Tuple<int>, IEquatable<IdentityTuple>
            {
                public IdentityTuple(int value) : base(value)
                {
                }
                bool IEquatable<IdentityTuple>.Equals(IdentityTuple other) => ReferenceEquals(this, other);
            }
            public sealed class TracedTuple : Tuple<int>, IEquatable<TracedTuple>
            {
                public static int TypedEqualsCalls;
                public TracedTuple(int value) : base(value)
                {
                }
                bool IEquatable<TracedTuple>.Equals(TracedTuple other)
                {
                    TypedEqualsCalls++;
                    return other != null && Item1 == other.Item1;
                }
            }
            public static class ArgumentProbe
            {
                private static readonly List<string> Trace = new List<string>();
                public static IEqualityComparer<object> Comparer
                {
                    get
                    {
                        Trace.Add("comparer");
                        return EqualityComparer<object>.Default;
                    }
                }
                public static IEqualityComparer<object> ThrowingComparer
                {
                    get
                    {
                        Trace.Add("comparer");
                        throw new InvalidOperationException();
                    }
                }
                public static object[] Values
                {
                    get
                    {
                        Trace.Add("source");
                        return new object[] { 1L, 1L, 2L };
                    }
                }
                public static object[] ThrowingValues
                {
                    get
                    {
                        Trace.Add("source");
                        throw new InvalidOperationException();
                    }
                }
                public static Func<dynamic, dynamic> Selector
                {
                    get
                    {
                        Trace.Add("selector");
                        return value => value;
                    }
                }
                public static Func<dynamic, IEnumerable<dynamic>> ManySelector
                {
                    get
                    {
                        Trace.Add("selector");
                        return value => new object[] { value };
                    }
                }
                public static Func<int, int> IntSelector
                {
                    get
                    {
                        Trace.Add("selector");
                        return value => value;
                    }
                }
                public static int Start
                {
                    get
                    {
                        Trace.Add("start");
                        return 0;
                    }
                }
                public static IEqualityComparer<int> IntComparer
                {
                    get
                    {
                        Trace.Add("comparer");
                        return EqualityComparer<int>.Default;
                    }
                }
                public static int[] IntValues
                {
                    get
                    {
                        Trace.Add("source");
                        return new[] { 1, 1, 2 };
                    }
                }
                public static string Run(Func<int> action)
                {
                    Trace.Clear();
                    string result;
                    try
                    {
                        result = action().ToString();
                    }
                    catch (Exception e)
                    {
                        result = e.GetType().Name;
                    }
                    return string.Join(",", Trace) + ":" + result;
                }
                internal static string Order(Func<int> action)
                {
                    Run(action);
                    return string.Join(",", Trace.Distinct());
                }
                internal static int MostEvaluations(Func<int> action)
                {
                    Run(action);
                    return Trace.GroupBy(name => name).Max(names => names.Count());
                }
            }
            public sealed class ParityComparer : IEqualityComparer<int>
            {
                bool IEqualityComparer<int>.Equals(int x, int y) => x % 2 == y % 2;
                int IEqualityComparer<int>.GetHashCode(int value) => value % 2;
            }
            public static class TypedConsumers
            {
                internal static int Sum(IEnumerable<int> values) => values.Sum();
                internal static int SumAll(params IEnumerable<int>[] values) => values.Sum(x => x.Sum());
            }
            public sealed class Holder
            {
                public IEnumerable<int> Values;
                public IEnumerable<int> Items { get; set; }
            }
            public static class Overloads
            {
                internal static int Pick(IEnumerable<int> values) => 1;
                internal static int Pick(IEnumerable<object> values) => 2;
                internal static int PickElement(IEnumerable<int> values) => 1;
                internal static int PickElement(IEnumerable<string> values) => 2;
                internal static int PickWith(IEnumerable<int> values, Func<int, int> selector) => 1;
                internal static int PickWith(IEnumerable<object> values, Func<object, object> selector) => 2;
            }
            public static class DeferredProbe
            {
                private static readonly List<string> Trace = new List<string>();
                internal static IEnumerable<int> Values()
                {
                    Trace.Add("source");
                    return Enumerate(throws: false);
                }
                internal static IEnumerable<int> Throwing() => Enumerate(throws: true);
                private static IEnumerable<int> Enumerate(bool throws)
                {
                    Trace.Add("enumerated");
                    if (throws)
                        throw new InvalidOperationException();
                    yield return 1;
                    yield return 1;
                    yield return 2;
                }
                internal static string Run(Func<IEnumerable<int>> distinct) => Observe(distinct);
                internal static string RunObjects(Func<IEnumerable<object>> distinct) => Observe(distinct);
                private static string Observe<T>(Func<IEnumerable<T>> distinct)
                {
                    Trace.Clear();
                    var result = distinct();
                    Trace.Add("returned");
                    Trace.Add(result.Count().ToString());
                    return string.Join(",", Trace);
                }
            }
            public sealed class ConvertibleSequence : DynamicObject, IEnumerable<object>
            {
                public static int TryConvertCalls;
                private readonly object[] _values = new object[3];
                public static object Create()
                {
                    return new ConvertibleSequence();
                }
                public IEnumerator<object> GetEnumerator()
                {
                    return ((IEnumerable<object>)_values).GetEnumerator();
                }
                IEnumerator IEnumerable.GetEnumerator() => _values.GetEnumerator();
                public override bool TryConvert(ConvertBinder binder, out dynamic result)
                {
                    TryConvertCalls++;
                    result = null;
                    return false;
                }
            }
            public sealed class ImplicitSequence : DynamicObject
            {
                public static object Create()
                {
                    return new ImplicitSequence();
                }
                public override bool TryConvert(ConvertBinder binder, out dynamic result)
                {
                    if (binder.Explicit == false && binder.Type == typeof(IEnumerable<object>))
                    {
                        result = new object[2];
                        return true;
                    }
                    result = null;
                    return false;
                }
            }
            public static class CountProbe
            {
                public static int Calls;
                public static bool Reset()
                {
                    Calls = 0;
                    return true;
                }
                public static object Next()
                {
                    Calls++;
                    return new object[3];
                }
                public static object NextExplicit()
                {
                    Calls++;
                    return ExplicitSequence.Create();
                }
            }
            public sealed class ExplicitSequence : DynamicObject
            {
                private readonly object[] _values;
                private ExplicitSequence(object[] values)
                {
                    _values = values;
                }
                public static object Create()
                {
                    return new ExplicitSequence(new object[2]);
                }
                public override bool TryConvert(ConvertBinder binder, out dynamic result)
                {
                    if (binder.Explicit && binder.Type == typeof(IEnumerable<object>))
                    {
                        result = _values;
                        return true;
                    }
                    result = null;
                    return false;
                }
            }
            public static class ErrorProbe
            {
                public static string ParamName(Func<bool> action)
                {
                    try
                    {
                        action();
                        return null;
                    }
                    catch (ArgumentNullException e)
                    {
                        return e.ParamName;
                    }
                }
                public static string ExceptionType(Func<bool> action)
                {
                    try
                    {
                        action();
                        return null;
                    }
                    catch (Exception e)
                    {
                        return e.GetType().Name;
                    }
                }
            }
            public sealed class TypedSequenceValues : DynamicArray, IEnumerable<short>
            {
                private readonly short[] _values;
                public readonly List<string> Trace = new List<string>();
                public TypedSequenceValues(IEnumerable values) : this(values.Cast<object>().Select(value => (short)Convert.ToInt32(value)).ToArray())
                {
                }
                private TypedSequenceValues(short[] values) : base(values.Cast<object>().ToArray())
                {
                    _values = values;
                }
                IEnumerator<short> IEnumerable<short>.GetEnumerator()
                {
                    Trace.Add("short.GetEnumerator");
                    return EnumerateShorts().GetEnumerator();
                }
                private IEnumerable<short> EnumerateShorts()
                {
                    try
                    {
                        foreach (short value in _values)
                        {
                            Trace.Add("short.MoveNext:" + value);
                            yield return value;
                        }
                    }
                    finally
                    {
                        Trace.Add("short.Dispose");
                    }
                }
            }
            """;

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
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count(Enumerable.Concat(item.Tags, new ConcatCountOnlyCollection(2))) == 4) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Enumerable.Count(item.Tags, tag => Enumerable.Count((object)Enumerable.Concat(new ConcatCountOnlyCollection(1), new ConcatCountOnlyCollection(2))) == 3) == 2", SearchEngineMode = RavenSearchEngineMode.All)]
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
