using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs.Async
{
	using System.Linq;
	using Client.Linq;
	using Document;

	public class Querying : RemoteClientTest
    {
        [Fact]
        public void Can_query_using_async_session()
        {
            using(GetNewServer())
            using(var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                using (var s = store.OpenAsyncSession())
                {
                    s.Store(new {Name = "Ayende"});
                    s.SaveChangesAsync().Wait();
                }

                using(var s = store.OpenAsyncSession())
                {
                    var queryResultAsync = s.Advanced.AsyncLuceneQuery<dynamic>()
                        .WhereEquals("Name", "Ayende")
                        .QueryResultAsync;

                    queryResultAsync.Wait();

                    Assert.Equal("Ayende",
                        queryResultAsync.Result.Results[0].Value<string>("Name"));
                }
            }
        }

		[Fact]
		public void Can_perform_a_projection_in_a_linq_query()
		{
			using(GetNewServer())
            using(var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {

            	var entity = new Company {Name = "Simple Company", Id = "companies/1"};
            	using (var session = store.OpenSession())
            	{
            		session.Store(entity);
            		session.SaveChanges();
            	}

            	using (var session = store.OpenSession())
            	{
            		var results = session.Query<Company>()
						.Where(x => x.Name == "Simple Company")
            			.Select(x => x.Name)
						.ToList();

            		Assert.Equal(1, results.Count);
					Assert.Equal("Simple Company", results[0]);
            	}
            }
		}
    }
}