using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Database.Server;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class QueryingWithDynamicRavenQueryInspector : RemoteClientTest
	{
		private string path;
		private int port;

		public QueryingWithDynamicRavenQueryInspector()
		{
			port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);
		}

		[Fact()]
		public void CanInitializeDynamicRavenQueryInspector()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos"
			};

			using (var server = GetNewServer(port, path))
			{
				using (var store = new DocumentStore { Url = "http://localhost:" + port })
				{
					store.Initialize();

					using (var s = store.OpenSession())
					{
						s.Store(blogOne);
						s.Store(blogTwo);
						s.Store(blogThree);
						s.SaveChanges();
					}

					using (var s = store.OpenSession())
					{
						var blogs = s.Query<Blog>().AsQueryable();

						var blogQuery = from b in blogs
						                where b.Title == "two"
						                select b;

						var results = blogs.Provider.CreateQuery(blogQuery.Expression).As<Blog>().ToArray();
						Assert.True(results.Any(x => x.Title == "two"));
					}
				}
			}
		}

	}
}