using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class LoadDocumentEnumerableDisposalTests : RavenTestBase
    {
        public LoadDocumentEnumerableDisposalTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string TrackingEnumerableSource = @"
using System;
using System.Collections;

namespace DisposalTracking
{
    public sealed class TrackingEnumerable : IEnumerable, IDisposable
    {
        private readonly object[] _items;

        public bool Disposed;
        public bool EnumeratorDisposed;

        public TrackingEnumerable(object[] items)
        {
            _items = items;
        }

        public IEnumerator GetEnumerator()
        {
            return new Enumerator(this);
        }

        public void Dispose()
        {
            Disposed = true;
        }

        private sealed class Enumerator : IEnumerator, IDisposable
        {
            private readonly TrackingEnumerable _parent;
            private int _index = -1;

            public Enumerator(TrackingEnumerable parent)
            {
                _parent = parent;
            }

            public object Current
            {
                get { return _parent._items[_index]; }
            }

            public bool MoveNext()
            {
                return ++_index < _parent._items.Length;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public void Dispose()
            {
                _parent.EnumeratorDisposed = true;
            }
        }
    }
}
";

        private class Result
        {
            public string LoadedCompany { get; set; }
            public string DocumentEnumeratorDisposed { get; set; }
            public string DocumentEnumerableDisposed { get; set; }
            public string CompareExchangeEnumeratorDisposed { get; set; }
            public string CompareExchangeEnumerableDisposed { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LoadingFromAnEnumerableDisposesItsEnumeratorButNotTheEnumerable(Options options)
        {
            using var store = GetDocumentStore(options);

            var commands = store.Commands();
            commands.Put("companies/1", null, new { Name = "company" }, new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = "Companies" });
            commands.Put("orders/1", null, new { Company = "companies/1" }, new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = "Orders" });

            const string indexName = "Orders/LoadFromEnumerable";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps =
                {
                    @"from o in docs.Orders
let documentKeys = new DisposalTracking.TrackingEnumerable(new object[] { o.Company })
let companies = LoadDocument(documentKeys, ""Companies"")
let compareExchangeKeys = new DisposalTracking.TrackingEnumerable(new object[] { ""missing"" })
let values = LoadCompareExchangeValue(compareExchangeKeys)
select new
{
    LoadedCompany = companies[0].Name,
    DocumentEnumeratorDisposed = documentKeys.EnumeratorDisposed ? ""yes"" : ""no"",
    DocumentEnumerableDisposed = documentKeys.Disposed ? ""yes"" : ""no"",
    CompareExchangeEnumeratorDisposed = compareExchangeKeys.EnumeratorDisposed ? ""yes"" : ""no"",
    CompareExchangeEnumerableDisposed = compareExchangeKeys.Disposed ? ""yes"" : ""no""
}"
                },
                AdditionalSources = new Dictionary<string, string> { ["DisposalTracking"] = TrackingEnumerableSource },
                Fields = { [Constants.Documents.Indexing.Fields.AllFields] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));

            Indexes.WaitForIndexing(store);

            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName }));
            Assert.Empty(errors.SelectMany(x => x.Errors));

            using (var session = store.OpenSession())
            {
                var result = session.Advanced
                    .RawQuery<Result>($"from index '{indexName}' select LoadedCompany, DocumentEnumeratorDisposed, DocumentEnumerableDisposed, " +
                                      "CompareExchangeEnumeratorDisposed, CompareExchangeEnumerableDisposed")
                    .Single();

                Assert.Equal("company", result.LoadedCompany);
                Assert.Equal("yes", result.DocumentEnumeratorDisposed);
                Assert.Equal("no", result.DocumentEnumerableDisposed);
                Assert.Equal("yes", result.CompareExchangeEnumeratorDisposed);
                Assert.Equal("no", result.CompareExchangeEnumerableDisposed);
            }
        }
    }
}
