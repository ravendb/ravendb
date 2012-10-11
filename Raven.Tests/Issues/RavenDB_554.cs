namespace Raven.Tests.Issues
{
	using System.IO;
	using System.Net;
	using System.Threading;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Extensions;
	using Raven.Abstractions.Indexing;
	using Raven.Client.Document;

	using Xunit;

	public class RavenDB_554 : RemoteClientTest
	{
		public class Person
		{
			public string FirstName { get; set; }

			public string LastName { get; set; }

			public string MiddleName { get; set; }
		}

		[Fact]
		public void IndexEntryFieldShouldNotContainNullValues()
		{
			const string IndexName = "Index1";

			using (var server = GetNewServer())
			{
				server.Database.PutIndex(IndexName, new IndexDefinition
				{
					Map = "from doc in docs select new { doc.FirstName, doc.LastName, doc.MiddleName, Query = new[] { doc.FirstName, doc.LastName, doc.MiddleName } }"
				});

				using (var docStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
				{
					using (var session = docStore.OpenSession())
					{
						session.Store(new Person { FirstName = "John", MiddleName = null, LastName = null });
						session.Store(new Person { FirstName = "William", MiddleName = "Edgard", LastName = "Smith" });
						session.Store(new Person { FirstName = "Paul", MiddleName = null, LastName = "Smith" });
						session.SaveChanges();
					}

					QueryResult queryResult;
					do
					{
						Thread.Sleep(1000);

						var request = (HttpWebRequest)WebRequest.Create(string.Format("http://localhost:8079/indexes/{0}?query=&start=0&pageSize=128&aggregation=None&debug=entries", IndexName));
						request.Method = "GET";

						using (var resp = request.GetResponse())
						using (var stream = resp.GetResponseStream())
						{
							var reader = new StreamReader(stream);
							queryResult = reader.JsonDeserialization<QueryResult>();
						}
					}
					while (queryResult.IsStale);

					foreach (var result in queryResult.Results)
					{
						var q = result["Query"].ToString();
						Assert.NotNull(q);
						Assert.False(q.Contains(Constants.NullValue));
					}
				}
			}
		}
	}
}