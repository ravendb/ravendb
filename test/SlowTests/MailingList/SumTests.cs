// -----------------------------------------------------------------------
//  <copyright file="Ben.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class SumTests : RavenTestBase
    {
        private class Vacancy
        {
            public string Id { get; set; }
            public string Position { get; set; }
        }

        private class VacancyApplication
        {
            public string Id { get; set; }
            public string VacancyId { get; set; }
            public string State { get; set; }
        }

        private class Vacancies_ApplicationCount : AbstractIndexCreationTask<VacancyApplication, Vacancies_ApplicationCount.ReduceResult>
        {
            public Vacancies_ApplicationCount()
            {
                Map = applications => from a in applications
                                      select new
                                      {
                                          Id = a.VacancyId,
                                          State = a.State,
                                          ApplicationCount = 1
                                      };

                Reduce = results => from result in results
                                    group result by new { result.Id, result.State } into g
                                    select new
                                    {
                                        Id = g.Key.Id,
                                        State = g.Key.State,
                                        ApplicationCount = g.Sum(x => x.ApplicationCount)
                                    };
            }

            public class ReduceResult
            {
                public string Id { get; set; }
                public int ApplicationCount { get; set; }
                public string State { get; set; }
            }
        }

        private class Vacancies_ApplicationCountTransformer : AbstractTransformerCreationTask<Vacancies_ApplicationCount.ReduceResult>
        {
            public Vacancies_ApplicationCountTransformer()
            {
                TransformResults = results => from result in results
                                              group result by result.Id into g
                                              let vacancy = LoadDocument<Vacancy>(g.Key)
                                              select new
                                              {
                                                  Id = g.Key,
                                                  ApplicationCount = g.Sum(x => Convert.ToInt32(x.ApplicationCount))
                                              };
            }

            public class ReduceResult
            {
                public string Id { get; set; }
                public int ApplicationCount { get; set; }
                public string State { get; set; }
            }
        }

        private readonly string _vacancyId;

        private readonly IDocumentStore _store;

        public SumTests()
        {
            _store = GetDocumentStore();
            new Vacancies_ApplicationCount().Execute(_store);
            new Vacancies_ApplicationCountTransformer().Execute(_store);
            using (var session = _store.OpenSession())
            {
                var vacancy = new Vacancy { Position = "Developer Guy" };
                session.Store(vacancy);

                var app1 = new VacancyApplication { VacancyId = vacancy.Id, State = "Approved" };
                var app2 = new VacancyApplication { VacancyId = vacancy.Id, State = "Unapproved" };

                session.Store(app1);
                session.Store(app2);

                session.SaveChanges();

                _vacancyId = vacancy.Id;
            }
        }

        [Fact(Skip = "TODO Arek")]
        public void Can_get_application_counts_by_vacancy_id()
        {
            using (var session = _store.OpenSession())
            {
                var results = session.Query<Vacancies_ApplicationCount.ReduceResult, Vacancies_ApplicationCount>()
                    .TransformWith<Vacancies_ApplicationCountTransformer, Vacancies_ApplicationCount.ReduceResult>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .ToList();
                Assert.Equal(results.First().Id, _vacancyId);
                Assert.Equal(results.First().ApplicationCount, 2);
            }
        }

        [Fact(Skip = "TODO Arek")]
        public void Can_get_application_counts_by_state()
        {
            using (var session = _store.OpenSession())
            {
                var results = session.Query<Vacancies_ApplicationCount.ReduceResult, Vacancies_ApplicationCount>()
                    .TransformWith<Vacancies_ApplicationCountTransformer, Vacancies_ApplicationCount.ReduceResult>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.State == "Approved")
                    .ToList();

                Assert.Equal(results.First().Id, _vacancyId);
                Assert.Equal(results.First().ApplicationCount, 1);
            }
        }
    }
}
