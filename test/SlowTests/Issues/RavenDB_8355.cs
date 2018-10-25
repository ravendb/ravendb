using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
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

        [Fact]
        public void CanUseCustomSorter()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, string>
                {
                    { "MySorter", SorterCode}
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C1" });
                    session.Store(new Company { Name = "C2" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() =>
                    {
                        session
                            .Advanced
                            .RawQuery<Company>("from Companies order by custom(Name, 'MySorter')")
                            .ToList();
                    });

                    Assert.Contains("Catch me: Name:2:0:False", e.Message);

                    e = Assert.Throws<RavenException>(() =>
                    {
                        session
                            .Query<Company>()
                            .OrderBy(x => x.Name, "MySorter")
                            .ToList();
                    });

                    Assert.Contains("Catch me: Name:2:0:False", e.Message);

                    e = Assert.Throws<RavenException>(() =>
                    {
                        session
                            .Advanced
                            .DocumentQuery<Company>()
                            .OrderBy(x => x.Name, "MySorter")
                            .ToList();
                    });

                    Assert.Contains("Catch me: Name:2:0:False", e.Message);

                    e = Assert.Throws<RavenException>(() =>
                    {
                        session
                            .Advanced
                            .RawQuery<Company>("from Companies order by custom(Name, 'MySorter') desc")
                            .ToList();
                    });

                    Assert.Contains("Catch me: Name:2:0:True", e.Message);

                    e = Assert.Throws<RavenException>(() =>
                    {
                        session
                            .Query<Company>()
                            .OrderByDescending(x => x.Name, "MySorter")
                            .ToList();
                    });

                    Assert.Contains("Catch me: Name:2:0:True", e.Message);

                    e = Assert.Throws<RavenException>(() =>
                    {
                        session
                            .Advanced
                            .DocumentQuery<Company>()
                            .OrderByDescending(x => x.Name, "MySorter")
                            .ToList();
                    });

                    Assert.Contains("Catch me: Name:2:0:True", e.Message);
                }
            }
        }
    }
}
