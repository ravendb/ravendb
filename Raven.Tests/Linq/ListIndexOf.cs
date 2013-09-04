using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Linq
{
	public class ListIndexOf : RavenTest
	{
		[Fact]
		public void CanUseIndexOf()
		{
			using (var store = NewDocumentStore())
			{
				new ProjectsIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					var portfolio = new Portfolio { Id = "sites/1/portfolio" };
					var p1 = new Project { Title = "Project 1", SiteId = "sites/1" };
					var p2 = new Project { Title = "Project 2", SiteId = "sites/1" };

					session.Store(p1);
					session.Store(p2);
					portfolio.Projects.AddRange(new[] { p2.Id, p1.Id });
					session.Store(portfolio);

					session.SaveChanges();
				}

				WaitForIndexing(store);
				AssertNoIndexErrors(store);

				using (var session = store.OpenSession())
				{
					var results = session.Advanced.LuceneQuery<Project, ProjectsIndex>()
						.OrderBy("PortfolioIndex")
						.ToList();

					Assert.Equal("Project 2", results.First().Title);
				}
			}
		}

		public class Project
		{
			public string Id { get; set; }
			public string SiteId { get; set; }
			public string Title { get; set; }
		}

		public class Portfolio
		{
			public string Id { get; set; }
			public List<string> Projects { get; set; }

			public Portfolio()
			{
				Projects = new List<string>();
			}
		}

		public class ProjectsIndex : AbstractIndexCreationTask<Project>
		{
			public ProjectsIndex()
			{
				Map = projects => from p in projects
								  let portfolio = LoadDocument<Portfolio>(p.SiteId + "/portfolio")
								  select new
								  {
									  Id = p.Id,
									  Title = p.Title,
									  SiteId = p.SiteId,
									  PortfolioIndex = portfolio.Projects.IndexOf(p.Id)
								  };
			}
		}
	}
}