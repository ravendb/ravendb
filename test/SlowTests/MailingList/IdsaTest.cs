// -----------------------------------------------------------------------
//  <copyright file="IdsaTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class IdsaTest : RavenTestBase
    {
        public IdsaTest(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void LuceneCanGetEmptyCollection(Options options) => CanGetEmptyCollection(options, list =>
        {
            Assert.Null(list.Single().Exemptions);
        });

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Skip = "Corax")]
        public void CoraxCanGetEmptyCollection(Options options) => CanGetEmptyCollection(options, list =>
        {
            Assert.Empty(list.Single().Exemptions);
        });

        private void CanGetEmptyCollection(Options options, Action<List<CasinosSuspensionsIndex.IndexResult>> assertion)
        {
            using (var store = GetDocumentStore(options))
            {
                new CasinosSuspensionsIndex().Execute(store);

                using (var documentSession = store.OpenSession())
                {
                    var casino = new Casino("cities/1", "address", "name")
                    {
                        Suspensions = new List<Suspension>
                        {
                            new Suspension(DateTime.UtcNow, new List<Exemption>())
                        }
                    };
                    documentSession.Store(casino);
                    documentSession.SaveChanges();

                    var suspensions = documentSession.Query<CasinosSuspensionsIndex.IndexResult, CasinosSuspensionsIndex>().
                        Customize(x => x.WaitForNonStaleResults()).
                        Where(x => x.CityId == "cities/1").
                        OrderByDescending(x => x.DateTime).
                        Take(10).
                        ProjectInto<CasinosSuspensionsIndex.IndexResult>().
                        ToList();

                    // note that suspensions[0].Exemptions will be null, because we don't have
                    // any values in the array, and we don't store empty arrays
                    Assert.NotEmpty(suspensions);
                    assertion(suspensions);
                }
            }
        }

        private class CasinosSuspensionsIndex : AbstractIndexCreationTask<Casino, CasinosSuspensionsIndex.IndexResult>
        {
            public class IndexResult
            {
                public string CityId { get; set; }
                public string CasinoId { get; set; }
                public string CasinoAddress { get; set; }
                public string Id { get; set; }
                public DateTime DateTime { get; set; }
                public IList<Exemption> Exemptions { get; set; }
            }

            public CasinosSuspensionsIndex()
            {
                Map = casinos => from casino in casinos
                                 from suspension in casino.Suspensions
                                 select new
                                 {
                                     CityId = casino.CityId,
                                     CasinoId = casino.Id,
                                     CasinoAddress = casino.Address,
                                     Id = suspension.Id,
                                     DateTime = suspension.DateTime,
                                     Exemptions = (object[])suspension.Exemptions ?? new object[0]
                                 };

                Store(x => x.CityId, FieldStorage.Yes);
                Store(x => x.CasinoId, FieldStorage.Yes);
                Store(x => x.CasinoAddress, FieldStorage.Yes);
                Store(x => x.Id, FieldStorage.Yes);
                Store(x => x.DateTime, FieldStorage.Yes);
                Store(x => x.Exemptions, FieldStorage.Yes);
            }
        }

        private class Casino
        {
            public string Id { get; set; }
            public DateTime AdditionDate { get; set; }
            public string CityId { get; set; }
            public string Address { get; set; }
            public string Title { get; set; }
            public CasinoStatus Status { get; set; }
            public IList<Comment> Comments { get; set; }
            public IList<Suspension> Suspensions { get; set; }

            private Casino()
            {
                Status = CasinoStatus.Opened;

                Comments = new List<Comment>();
                Suspensions = new List<Suspension>();
            }

            public Casino(string cityId, string address, string name)
                : this()
            {
                AdditionDate = DateTime.UtcNow;
                CityId = cityId;
                Address = address;
                Title = name;
            }
        }

        private enum CasinoStatus
        {
            Opened = 1,
            Closed = 2
        }

        private class Suspension
        {
            public string Id { get; set; }
            public DateTime DateTime { get; set; }
            public IList<Exemption> Exemptions { get; set; }

            public Suspension(DateTime dateTime, IList<Exemption> exemptions)
            {
                DateTime = dateTime;
                Exemptions = exemptions;
            }
        }

        private class Exemption
        {
            public ExemptionItemType ItemType { get; set; }
            public long Quantity { get; set; }

            public Exemption(ExemptionItemType itemType, long quantity)
            {
                ItemType = itemType;
                Quantity = quantity;
            }
        }

        private enum ExemptionItemType
        {
            Unknown = 1,
            Pc = 2,
            SlotMachine = 3,
            Table = 4,
            Terminal = 5
        }

        private class Comment
        {
            public string Id { get; set; }
            public DateTime DateTime { get; set; }
            public string Author { get; set; }
            public string Text { get; set; }

            public Comment(string author, string text)
            {
                DateTime = DateTime.UtcNow;
                Author = author;
                Text = text;
            }
        }
    }
}
