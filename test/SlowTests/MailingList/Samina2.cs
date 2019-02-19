// -----------------------------------------------------------------------
//  <copyright file="Samina2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Samina2 : RavenTestBase
    {
        private class PropertySearchingViewModel
        {
            public string Id { get; set; }
            public string UserFriendlyId { get; set; }
            public List<Unavailability> Unavailabilities { get; set; }
            public string UserFriendlyPropertyId { get; set; }

            public PropertySearchingViewModel()
            {
                Unavailabilities = new List<Unavailability>();
            }
        }

        private class Unavailability
        {
            public DateTime StartDay { get; set; }
            public DateTime EndDay { get; set; }
        }

        [Fact]
        public void Querying_a_sub_collection_in_an_index()
        {
            DateTime startDate = DateTime.Now;
            DateTime endDate = DateTime.Now.AddDays(10);

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var model = new PropertySearchingViewModel() { Id = Guid.NewGuid().ToString(), UserFriendlyPropertyId = "p001" };
                    model.Unavailabilities.Add(new Unavailability() { StartDay = startDate, EndDay = endDate });

                    session.Store(model);
                    session.SaveChanges();
                }

                new PropertiesSearchIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var count = session.Query<PropertySearchingViewModel, PropertiesSearchIndex>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Count(x => x.Unavailabilities.Any(y => y.StartDay >= startDate && y.EndDay <= endDate));

                    Assert.Equal(1, count);
                    Assert.Equal("PropertiesSearchIndex", stats.IndexName);
                }
            }
        }

        private class PropertiesSearchIndex : AbstractIndexCreationTask<PropertySearchingViewModel>
        {
            public PropertiesSearchIndex()
            {
                Map = items =>
                      from propertySearchingViewModel in items
                      from searchingViewModel in propertySearchingViewModel.Unavailabilities
                      select
                        new
                        {
                            Unavailabilities_StartDay = searchingViewModel.StartDay,
                            Unavailabilities_EndDay = searchingViewModel.EndDay
                        };

            }
        }
    }
}
