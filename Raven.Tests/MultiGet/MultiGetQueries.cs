using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Bugs.TransformResults;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;
using User = Raven.Tests.Linq.User;

namespace Raven.Tests.MultiGet
{
	public class MultiGetQueries : RavenTest
	{
		[Fact]
		public void UnlessAccessedLazyQueriesAreNoOp()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.Equal(0, session.Advanced.NumberOfRequests);
				}

			}
		}

		[Fact]
		public void WithPaging()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Age == 0).Skip(1).Take(2).Lazily();
					Assert.Equal(2, result1.Value.ToArray().Length);
				}

			}
		}


		[Fact]
		public void CanGetQueryStats()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User{Age = 3});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats1;
					var result1 = session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Statistics(out stats1)
						.Where(x => x.Age == 0).Skip(1).Take(2)
						.Lazily();

					RavenQueryStatistics stats2;
					var result2 = session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Statistics(out stats2)
						.Where(x => x.Age == 3).Skip(1).Take(2)
						.Lazily();
					Assert.Equal(2, result1.Value.ToArray().Length);
					Assert.Equal(3, stats1.TotalResults);

					Assert.Equal(0, result2.Value.ToArray().Length);
					Assert.Equal(1, stats2.TotalResults);
				}

			}
		}

		[Fact]
		public void WithQueuedActions()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					IEnumerable<User> users = null;
					session.Query<User>().Where(x => x.Age == 0).Skip(1).Take(2).Lazily(x => users = x);
					session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					Assert.Equal(2, users.Count());
				}

			}
		}

		[Fact]
		public void WithQueuedActions_Load()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					User user = null;
					session.Advanced.Lazily.Load<User>("users/1", x => user = x);
					session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					Assert.NotNull(user);
				}

			}
		}

		[Fact]
		public void write_then_read_from_complex_entity_types_lazily()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new Answers_ByAnswerEntity().Execute(store);

				string answerId = ComplexValuesFromTransformResults.CreateEntities(store);
				// Working
				using (IDocumentSession session = store.OpenSession())
				{
					var answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Id == answerId)
						.As<AnswerEntity>()
						.Lazily();
					Assert.NotNull(answerInfo.Value.ToArray().Length);
				}
			}
		}

		[Fact]
		public void LazyOperationsAreBatched()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.Empty(result2.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.Empty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}

			}
		}

		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

				}

			}
		}

		[Fact]
		public void LazyWithProjection()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren")
						.Select(x => new { x.Name })
						.Lazily();

					Assert.Equal("oren", result1.Value.First().Name);
				}

			}
		}


		[Fact]
		public void LazyWithProjection2()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren")
						.Select(x => new { x.Name })
						.ToArray();

					Assert.Equal("oren", result1.First().Name);
				}

			}
		}

		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession_WithNonStaleResponse()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Query<User>().ToArray();

					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

				}

			}
		}

		[Fact]
		public void CanGetStatisticsWithLazyQueryResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "test")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					RavenQueryStatistics stats2;
					var result1 = session.Query<User>().Statistics(out stats).Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Statistics(out stats2).Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);

					Assert.Equal(1, stats.TotalResults);
					Assert.Equal(1, stats2.TotalResults);
				}

			}
		}
	}
}