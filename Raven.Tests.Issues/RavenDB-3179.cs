using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder.Extensions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Listeners;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3179 :RavenTestBase
    {
		[Fact]
		public async Task To_Facet_Lazy_Async()
		{
			using (var store = NewDocumentStore())
			{
				new AdviceSearch().Execute(store);
				using (var session = store.OpenSession())
				{
					SetupFacets(session);
					GenerateData(session);
					session.SaveChanges();
				}

				WaitForIndexing(store);
			
				using (var session = store.OpenAsyncSession())
				{
					RavenQueryStatistics stats;
					var query = session.Query<AdviceSearch.Result, AdviceSearch>()
						.Where(x => x.Sections == "Sections/1")
						.Search(x => x.SearchField, "How to", options: SearchOptions.And)
						.Include(x => x.Sections);

		
					var facetResultsLazy = query.ToFacetsLazyAsync("facets/ArticleFacets");

					var articleResults = query
						.Customize(x => x.ShowTimings())
						.Statistics(out stats)
						.OrderByScoreDescending()
						.As<Article>()
						.LazilyAsync();


					await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					

					var facetResults = await facetResultsLazy.Value;


					var sections =  facetResults.Results.FirstOrDefault().Value;

					  var results =  sections.Values.Select(async (value) =>
					  {
						  // Doesn't contact server due to Include in original query
						  var section = await session.LoadAsync<Section>(value.Range);
						  return new SectionFacet
						  {
							  Id = section.Id,
							  Name = section.Name,
							  Count = value.Hits
						  };
					  }).ToList();
					  await Task.WhenAll(results);
					
					 IList<SectionFacet> listSections = results.Select(x => x.Result).ToList();
				}
			}
		}

        public class Article
		{
			public int Id { get; set; }
			public string Title { get; set; }
			public string Description { get; set; }
			public string Content { get; set; }
			// List of section IDs the article is listed under
			public string[] Sections { get; set; }
		}

		public class Section
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Slug { get; set; }
		}
        public class AdviceSearch : AbstractIndexCreationTask<Article, AdviceSearch.Result>
		{
			public class Result
			{
				public string Id { get; set; }
				public string SearchField { get; set; }
				public string Sections { get; set; }
			}

			public AdviceSearch()
			{
				Map = articles =>
				      from article in articles
				      select new
				      {
					      article.Id,
					      SearchField = new object[]
					      {
						      article.Title,
						      article.Description,
						      article.Content
					      },
					      article.Sections
				      };

				Index(x => x.SearchField, FieldIndexing.Analyzed);
			}
		}

		public class SectionFacet
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public int Count { get; set; }
		}

		void SetupFacets(IDocumentSession session)
		{
			session.Store(new FacetSetup
			{
				Id = "facets/ArticleFacets",
				Facets = new[] { new Facet { Name = "Sections" } }.ToList()
			});
		}

		void GenerateData(IDocumentSession session)
		{
			var sections = new[]
			{
				new Section {Id = 1, Name = "TV Articles", Slug = "tv-articles"},
				new Section {Id = 2, Name = "General Articles", Slug = "general"}
			};
			var articles = new[]
			{
				new Article {Id = 1, Title = "How to fix your TV", Description = "How to", Sections = new[] {"Sections/1", "Sections/2"}},
				new Article {Id = 2, Title = "How to do something", Description = "How to", Sections = new[] {"Sections/2"}}
			};
			foreach (var section in sections) session.Store(section);
			foreach (var article in articles) session.Store(article);
		}
    }
}
