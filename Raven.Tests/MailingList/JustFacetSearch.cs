// -----------------------------------------------------------------------
//  <copyright file="JustFacetSearch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Listeners;
using Rhino.Mocks.Constraints;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class JustFacetSearch : RavenTest
	{
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

		public class Advice_Search : AbstractIndexCreationTask<Article, Advice_Search.Result>
		{
			public class Result
			{
				public string Id { get; set; }
				public string SearchField { get; set; }
				public string Sections { get; set; }
			}

			public Advice_Search()
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

		public class FacetSearcher
		{
			readonly IDocumentStore _store;

			public FacetSearcher(IDocumentStore store)
			{
				_store = store;
			}

			public IList<SectionFacet> FacetSearch(string searchTerm, int? sectionId = null, int page = 1, int pageSize = 10)
			{
				using (var session = _store.OpenSession())
				{
					RavenQueryStatistics stats;
					var query = session.Query<Advice_Search.Result, Advice_Search>()
						.Statistics(out stats)
						// Optimize loading of section facets
						.Include(x => x.Sections);

					// Filter by section
					if (sectionId != null)
					{
						var ravenId = "Sections/" + sectionId;
						query = query.Where(x => x.Sections == ravenId);
					}

					query = query.Search(x => x.SearchField, searchTerm, options: SearchOptions.And);

					var results = query
						.Skip((page - 1) * pageSize)
						.Take(pageSize)
						.As<Article>();

					// The following returns NO RESULTS unless I include: 
					// var something = results.ToList(); // Throw away list
					return getFacets(results, session);
				}
			}

			IList<SectionFacet> getFacets(IQueryable<Article> results, IDocumentSession session)
			{
				var facetResults = results.ToFacets("facets/ArticleFacets");
				var sections = facetResults.Results.FirstOrDefault().Value;
				return sections.Values.Select(value =>
				{
					// Doesn't contact server due to Include in original query
					var section = session.Load<Section>(value.Range);
					return new SectionFacet
					{
						Id = section.Id,
						Name = section.Name,
						Count = value.Hits
					};
				}).ToList();
			}
		}



		void setupFacets(IDocumentSession session)
		{
			session.Store(new FacetSetup
			{
				Id = "facets/ArticleFacets",
				Facets = new[] { new Facet { Name = "Sections" } }.ToList()
			});
		}

		void generateData(IDocumentSession session)
		{
			var sections = new[]
			{
				new Section {Id = 1, Name = "TV Articles", Slug = "tv-articles"},
				new Section {Id = 2, Name = "General Articles", Slug = "general"}
			};
			var articles = new[]
			{
				new Article
				{Id = 1, Title = "How to fix your TV", Description = "How to", Sections = new[] {"Sections/1", "Sections/2"}},
				new Article {Id = 2, Title = "How to do something", Description = "How to", Sections = new[] {"Sections/2"}}
			};
			foreach (var section in sections) session.Store(section);
			foreach (var article in articles) session.Store(article);
		}

		[Fact]
		public void JustReturnFacets()
		{
			using (var store = NewDocumentStore())
			{
				new Advice_Search().Execute(store);
				store.RegisterListener(new NoStaleQueriesListener());
				var searcher = new FacetSearcher(store);
				using (var session = store.OpenSession())
				{
					setupFacets(session);
					generateData(session);
					session.SaveChanges();
				}
				while (store.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
				{
					Thread.Sleep(100);
				}

				var facets = searcher.FacetSearch("how to");
				Assert.Equal(2, facets.Count);
				Assert.Equal("TV Articles", facets[0].Name);
				Assert.Equal("General Articles", facets[1].Name);
			}
		}

		public class NoStaleQueriesListener : IDocumentQueryListener
		{
			public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
			{
				queryCustomization.WaitForNonStaleResults();
			}
		}
	}
}