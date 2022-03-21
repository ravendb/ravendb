using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10469 : RavenTestBase
    {
        public RavenDB_10469(ITestOutputHelper output) : base(output)
        {
        }

        private class Index1 : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public DateTime SomeDate { get; set; }
            }

            public Index1()
            {
                Map = users => from user in users
                               select new
                               {
                                   SomeDate = DateTime.Parse("2018-02-02", CultureInfo.InvariantCulture)
                               };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void CanUseTypesFromSystemGlobalizationInIndexes()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var date = session.Query<Index1.Result, Index1>()
                        .Select(x => x.SomeDate)
                        .First();

                    Assert.Equal(DateTime.Parse("2018-02-02", CultureInfo.InvariantCulture), date);
                }
            }
        }
    }
}
