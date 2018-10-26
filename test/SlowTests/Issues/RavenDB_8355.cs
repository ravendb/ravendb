using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Sorters;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Sorters;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8355 : RavenTestBase
    {
        private const string SorterCode = @"
using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace SlowTests.Issues
{
    public class MySorter : FieldComparator
    {
        private readonly string _args;

        public MySorter(string fieldName, int numHits, int sortPos, bool reversed)
        {
            _args = $""{fieldName}:{numHits}:{sortPos}:{reversed}"";
        }

        public override int Compare(int slot1, int slot2)
        {
            throw new InvalidOperationException($""Catch me: {_args}"");
        }

        public override void SetBottom(int slot)
        {
            throw new InvalidOperationException($""Catch me: {_args}"");
        }

        public override int CompareBottom(int doc, IState state)
        {
            throw new InvalidOperationException($""Catch me: {_args}"");
        }

        public override void Copy(int slot, int doc, IState state)
        {
            throw new InvalidOperationException($""Catch me: {_args}"");
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            throw new InvalidOperationException($""Catch me: {_args}"");
        }

        public override IComparable this[int slot] => throw new InvalidOperationException($""Catch me: {_args}"");
    }
}
";

        private const string SorterCode2 = @"
using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace SlowTests.Issues
{
    public class MySorter : FieldComparator
    {
        private readonly string _args;

        public MySorter(string fieldName, int numHits, int sortPos, bool reversed)
        {
            _args = $""{fieldName}:{numHits}:{sortPos}:{reversed}:other"";
        }

        public override int Compare(int slot1, int slot2)
        {
            throw new InvalidOperationException($""Catch me 2: {_args}"");
        }

        public override void SetBottom(int slot)
        {
            throw new InvalidOperationException($""Catch me 2: {_args}"");
        }

        public override int CompareBottom(int doc, IState state)
        {
            throw new InvalidOperationException($""Catch me 2: {_args}"");
        }

        public override void Copy(int slot, int doc, IState state)
        {
            throw new InvalidOperationException($""Catch me 2: {_args}"");
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            throw new InvalidOperationException($""Catch me 2: {_args}"");
        }

        public override IComparable this[int slot] => throw new InvalidOperationException($""Catch me 2: {_args}"");
    }
}
";

        [Fact]
        public void CanUseCustomSorter()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                {
                    { "MySorter", new SorterDefinition
                    {
                        Name = "MySorter",
                        Code = SorterCode
                    }}
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C1" });
                    session.Store(new Company { Name = "C2" });

                    session.SaveChanges();
                }

                CanUseSorterInternal<RavenException>(store, "Catch me: Name:2:0:False", "Catch me: Name:2:0:True");
            }
        }

        [Fact]
        public void CanUseCustomSorterWithOperations()
        {
            using (var store = GetDocumentStore(new Options()))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C1" });
                    session.Store(new Company { Name = "C2" });

                    session.SaveChanges();
                }

                CanUseSorterInternal<SorterDoesNotExistException>(store, "There is no sorter with 'MySorter' name", "There is no sorter with 'MySorter' name");

                store.Maintenance.Send(new PutSortersOperation(new SorterDefinition
                {
                    Name = "MySorter",
                    Code = SorterCode
                }));

                // checking if we can send again same sorter
                store.Maintenance.Send(new PutSortersOperation(new SorterDefinition
                {
                    Name = "MySorter",
                    Code = SorterCode
                }));

                CanUseSorterInternal<RavenException>(store, "Catch me: Name:2:0:False", "Catch me: Name:2:0:True");

                // checking if we can update sorter
                store.Maintenance.Send(new PutSortersOperation(new SorterDefinition
                {
                    Name = "MySorter",
                    Code = SorterCode2
                }));

                CanUseSorterInternal<RavenException>(store, "Catch me 2: Name:2:0:False:other", "Catch me 2: Name:2:0:True:other");

                store.Maintenance.Send(new DeleteSorterOperation("MySorter"));

                CanUseSorterInternal<SorterDoesNotExistException>(store, "There is no sorter with 'MySorter' name", "There is no sorter with 'MySorter' name");
            }
        }

        private static void CanUseSorterInternal<TException>(DocumentStore store, string asc, string desc)
            where TException : RavenException
        {
            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .RawQuery<Company>("from Companies order by custom(Name, 'MySorter')")
                        .ToList();
                });

                Assert.Contains(asc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Query<Company>()
                        .OrderBy(x => x.Name, "MySorter")
                        .ToList();
                });

                Assert.Contains(asc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .DocumentQuery<Company>()
                        .OrderBy(x => x.Name, "MySorter")
                        .ToList();
                });

                Assert.Contains(asc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .RawQuery<Company>("from Companies order by custom(Name, 'MySorter') desc")
                        .ToList();
                });

                Assert.Contains(desc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Query<Company>()
                        .OrderByDescending(x => x.Name, "MySorter")
                        .ToList();
                });

                Assert.Contains(desc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .DocumentQuery<Company>()
                        .OrderByDescending(x => x.Name, "MySorter")
                        .ToList();
                });

                Assert.Contains(desc, e.Message);
            }
        }
    }
}
