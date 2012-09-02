using System;
using System.Linq;
using Raven.Abstractions;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class RacielrodTest : RemoteClientTest
	{
		[Fact]
		public void WhenNoQuery_CanOrderByNestedProperty()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"})
			{
				store.Initialize();
				using (IDocumentSession s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new
									{
										Id = "tests/" + i,
										Name = "Test" + i,
										Content = new
													  {
														  Order = i,
														  Inserted = SystemTime.UtcNow.AddDays(i)
													  }
									});
					}
					s.SaveChanges();
				}
				using (IDocumentSession s = store.OpenSession())
				{
					var objects =
						s.Advanced.LuceneQuery<dynamic>()
							.OrderBy("-Content.Order")
							.Take(2)
							.WaitForNonStaleResults()
							.ToArray();

					Assert.Equal(2, objects.Length);
					Assert.Equal(9,
									objects[0].Content.Order);
				}
			}
		}


		[Fact]
		public void WheQuery_CanOrderByNestedProperty()
		{
			using(GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"})
			{
				store.Initialize();
				using (IDocumentSession s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new
									{
										Id = "samples/" + i,
										Name = "Sample" + i,
										Body = new
												   {
													   Order = i,
													   Inserted = SystemTime.UtcNow.AddDays(i)
												   }
									});
					}
					s.SaveChanges();
				}
				using (IDocumentSession s = store.OpenSession())
				{
					IDocumentQuery<dynamic> query =
						s.Advanced.LuceneQuery<dynamic>()
							.WhereBetweenOrEqual("Body.Inserted",
												 SystemTime.UtcNow.Date, SystemTime.UtcNow.AddDays(2))
							.OrderBy("-Body.Order")
							.Take(2)
							.WaitForNonStaleResults();
					var objects = query.ToArray();

					Assert.Equal(2, objects.Length);
					Assert.Equal(3, query.QueryResult.TotalResults);
					Assert.Equal(2, objects[0].Body.Order);
				}
			}
		}
	}
}