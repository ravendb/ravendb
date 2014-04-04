using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class SpatialQueryWithTransformTests : RavenTestBase
    {
        [Fact]
        public void CanQuery()
        {
            using (var store = NewDocumentStore())
            {
                new VacanciesIndex().Execute(store);
                new ViewItemTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employer
                    {
                        Id = "employers/ivanov",
                        Locations = new[]
                                                          {
                                                              new Location
                                                                  {
                                                                      Key = "Kiev", 
                                                                      Lng = 30.52340, 
                                                                      Ltd = 50.45010
                                                                  }
                                                          }
                    });
                    session.Store(new Vacancy
                    {
                        Id = "employers/ivanov/vacancies/xy",
                        Name = "Test",
                        CompanyId = "employers/ivanov",
                        Owner = "employers/ivanov",
                        LocationId = "Kiev"
                    });
                    session.SaveChanges();
                }
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<VacanciesIndex.Result, VacanciesIndex>()
                                       .TransformWith<ViewItemTransformer, ViewItemTransformer.View>()
                                       .AddQueryInput("USERID", RavenJToken.FromObject("fake"))
                                       .Customize(x => x.WithinRadiusOf(10, 50.45010, 30.52340));
                    var result = query.ToList();
                    Assert.Equal(1, result.Count);
                }
            }
        }

        [Fact]
        public void CanQueryRemote()
        {
            using (var store = NewRemoteDocumentStore())
            {
                new VacanciesIndex().Execute(store);
                new ViewItemTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employer
                    {
                        Id = "employers/ivanov",
                        Locations = new[]
                                                          {
                                                              new Location
                                                                  {
                                                                      Key = "Kiev", 
                                                                      Lng = 30.52340, 
                                                                      Ltd = 50.45010
                                                                  }
                                                          }
                    });
                    session.Store(new Vacancy
                    {
                        Id = "employers/ivanov/vacancies/xy",
                        Name = "Test",
                        CompanyId = "employers/ivanov",
                        Owner = "employers/ivanov",
                        LocationId = "Kiev"
                    });
                    session.SaveChanges();
                }
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<VacanciesIndex.Result, VacanciesIndex>()
                                       .TransformWith<ViewItemTransformer, ViewItemTransformer.View>()
                                       .AddQueryInput("USERID", RavenJToken.FromObject("fake"))
                                       .Customize(x => x.WithinRadiusOf(10, 50.45010, 30.52340));
                    var result = query.ToList();
                    Assert.Equal(1, result.Count);
                }
            }
        }
    }

    public class Vacancy
    {
        public string Id { get; set; }
        public string Owner { get; set; }
        public string Name { get; set; }
        public string LocationId { get; set; }
        public string CompanyId { get; set; }
        public IEnumerable<string> Jobs { get; set; }
        public DateTimeOffset? CreatedAt { get; set; }
    }

    public class Employer
    {
        public string Id { get; set; }
        public string CompanyName { get; set; }
        public IList<Location> Locations { get; set; }
    }

    public class Location
    {
        public double Ltd { get; set; }
        public double Lng { get; set; }
        public string Key { get; set; }
    }

    public class ViewItemTransformer : AbstractTransformerCreationTask<Vacancy>
    {
        public const string USERID = "USERID";
        public class View
        {
            public string CompanyId { get; set; }
            public string Title { get; set; }
            public string Id { get; set; }
            public string Description { get; set; }
            public Location Location { get; set; }
        }

        public ViewItemTransformer()
        {
            TransformResults = results =>
                               from result in results
                               let employer = LoadDocument<Employer>(result.Owner)
                               let uid = Query(USERID)
                               let user = LoadDocument<Employer>(uid.ToString())
                               select new View
                               {
                                   Id = result.Id,
                                   Title = result.Name,
                                   Location = employer.Locations.FirstOrDefault(x => x.Key == result.LocationId)
                               };
        }
    }

    public class VacanciesIndex
       : AbstractIndexCreationTask<Vacancy, VacanciesIndex.Result>
    {
        public class Result
        {
            public string CompanyId { get; set; }
            public DateTimeOffset? CreatedAt { get; set; }
            public IEnumerable<object> Filter { get; set; }
            public string DocId { get; set; }
            public string Name { get; set; }
        }

        public VacanciesIndex()
        {
            Map = vacancies => from vacancy in vacancies
                               let owner = LoadDocument<Employer>(vacancy.CompanyId)
                               let location = owner.Locations.FirstOrDefault(x => x.Key == vacancy.LocationId)
                               select new
                               {
                                   DocId = vacancy.Id,
                                   CompanyId = vacancy.Owner,
                                   vacancy.Name,
                                   vacancy.CreatedAt,
                                   Filter = new object[]
                                    {
                                        vacancy.CompanyId,
                                        vacancy.Jobs
                                    },
                                   _ = SpatialGenerate(location.Ltd, location.Lng)
                               };
            Sort(x => x.CreatedAt, SortOptions.String);
        }
    }
}