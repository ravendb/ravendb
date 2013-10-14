// -----------------------------------------------------------------------
//  <copyright file="TimeSpanIndexing.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class TimeSpanIndexing : IDisposable
    {
        private IDocumentStore Store { get; set; }

        public TimeSpanIndexing()
        {
            Store = new Raven.Client.Embedded.EmbeddableDocumentStore {RunInMemory = true}
                .Initialize();
            new SimpleIndex().Execute(Store);
            var indexes = Store.DatabaseCommands.GetIndexNames(0, int.MaxValue);
            Assert.NotEmpty(indexes);
        }

        public void Dispose()
        {
            Store.Dispose();
        }

        [Fact]
        public void CanStoreAndQueryBasicValueWithIndex()
        {
            using (var session = Store.OpenSession())
            {
                const string id = "one";
                const string value = "value1";
                session.Store(new Simple {Id = id, Value = value});
                session.SaveChanges();

                var result =
                    session.Query<Simple, SimpleIndex>()
                           .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                           .FirstOrDefault(x => x.Id == id);
                Assert.NotNull(result);
                Assert.Equal(value, result.Value);
            }
        }

        [Fact]
        public void CanStoreTimeFormatInvalidValueAndQueryIndexForSameValue()
        {
            using (var session = Store.OpenSession())
            {
                const string id = "one";
                const string value = "12:62";
                session.Store(new Simple {Id = id, Value = value});
                session.SaveChanges();

                var result =
                    session.Query<Simple, SimpleIndex>()
                           .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                           .FirstOrDefault(x => x.Id == id);
                Assert.NotNull(result);
                Assert.Equal(value, result.Value);
            }
        }

        [Fact]
        public void CanStoreTimeFormatValidValueAndQueryIndexForSameValue()
        {
            using (var session = Store.OpenSession())
            {
                const string id = "one";
                const string value = "12:12";
                session.Store(new Simple {Id = id, Value = value});
                session.SaveChanges();

                var result =
                    session.Query<Simple, SimpleIndex>()
                           .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                           .FirstOrDefault(x => x.Id == id);
                Assert.NotNull(result);
                Assert.Equal(value, result.Value);
            }
        }

        public class Simple
        {
            public string Id { get; set; }
            public string Value { get; set; }
        }

        public class SimpleIndex : AbstractIndexCreationTask<Simple>
        {
            public SimpleIndex()
            {
                Map = mps => from s in mps
                             select new
                             {
                                 s.Id
                             };
                TransformResults = (db, results) => from r in results
                                                    let s = db.Load<Simple>(r.Id)
                                                    select new
                                                    {
                                                        r.Id,
                                                        s.Value
                                                    };
            }
        }
    }

}