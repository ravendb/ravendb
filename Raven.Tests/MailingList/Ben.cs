// -----------------------------------------------------------------------
//  <copyright file="Ben.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class RavenDbSumTests : RavenTest
	{
		public class Vacancy
		{
			public string Id { get; set; }
			public string Position { get; set; }
		}

		public class VacancyApplication
		{
			public string Id { get; set; }
			public string VacancyId { get; set; }
			public string State { get; set; }
		}

		public class Vacancies_ApplicationCount : AbstractIndexCreationTask<VacancyApplication, Vacancies_ApplicationCount.ReduceResult>
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

				TransformResults = (store, results) => from result in results
													   group result by result.Id into g
													   let vacancy = store.Load<Vacancy>(g.Key)
													   select new
													   {
														   Id = g.Key,
														   ApplicationCount = g.Sum(x => Convert.ToInt32(x.ApplicationCount))
													   };
			}

			public class ReduceResult
			{
				public string Id
				{
					get;
					set
;
				}
				public int ApplicationCount { get; set; }
				public string State { get; set; }
			}
		}

		static string vacancyId;

		private readonly EmbeddableDocumentStore store;

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		public RavenDbSumTests()
		{
			store = NewDocumentStore();
			new Vacancies_ApplicationCount().Execute(store);
			using (var session = store.OpenSession())
			{
				var vacancy = new Vacancy { Position = "Developer Guy" };
				session.Store(vacancy);

				var app1 = new VacancyApplication { VacancyId = vacancy.Id, State = "Approved" };
				var app2 = new VacancyApplication { VacancyId = vacancy.Id, State = "Unapproved" };

				session.Store(app1);
				session.Store(app2);

				session.SaveChanges();

				vacancyId = vacancy.Id;
			}
		}

		[Fact]
		public void Can_get_application_counts_by_vacancy_id()
		{
			using (var session = store.OpenSession())
			{
				var results = session.Query<Vacancies_ApplicationCount.ReduceResult, Vacancies_ApplicationCount>()
					.Customize(x => x.WaitForNonStaleResults())
					.ToList();
				Assert.Equal(results.First().Id, vacancyId);
				Assert.Equal(results.First().ApplicationCount, 2);
			}
		}

		[Fact]
		public void Can_get_application_counts_by_state()
		{
			using (var session = store.OpenSession())
			{
				var results = session.Query<Vacancies_ApplicationCount.ReduceResult, Vacancies_ApplicationCount>()
					.Customize(x => x.WaitForNonStaleResults())
					.Where(x => x.State == "Approved")
					.ToList();

				Assert.Equal(results.First().Id, vacancyId);
				Assert.Equal(results.First().ApplicationCount, 1);
			}
		}

	}
}