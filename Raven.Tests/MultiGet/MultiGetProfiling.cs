using System;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Xunit;
using Raven.Abstractions;

namespace Raven.Tests.MultiGet
{
	public class MultiGetProfiling : RemoteClientTest
	{
		[Fact]
		public void CanProfileLazyRequests()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Conventions.DisableProfiling = false;
				store.Initialize();
				using (var session = store.OpenSession())
				{
					// handle the initial request for replication information
				}
				Guid id;
				using (var session = store.OpenSession())
				{
					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
					session.Advanced.Lazily.Load<User>("users/1");
					session.Advanced.Lazily.Load<User>("users/2");
					session.Advanced.Lazily.Load<User>("users/3");

					session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
				}

				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result, Default.Converters);
				Assert.Equal(3, responses.Length);
				foreach (var response in responses)
				{
					Assert.Equal(404, response.Status);
				}

			}
		}

		[Fact]
		public void CanProfilePartiallyCachedLazyRequest()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Conventions.DisableProfiling = false; 
				store.Initialize();
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					session.Query<User>().Where(x => x.Name == "oren")
						.Customize(x => x.WaitForNonStaleResults())
						.ToArray();
				}
				Guid id;

				using (var session = store.OpenSession())
				{
					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
					session.Query<User>().Where(x => x.Name == "oren").Lazily();
					session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

				}
				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result, Default.Converters);
				Assert.Equal(304, responses[0].Status);
				Assert.Contains("oren", responses[0].Result.ToString());

				Assert.Equal(200, responses[1].Status);
				Assert.Contains("ayende", responses[1].Result.ToString());
			}
		}

		[Fact]
		public void CanProfileFullyCached()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Conventions.DisableProfiling = false; 
				store.Initialize();
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					session.Query<User>().Where(x => x.Name == "oren").ToArray();
					session.Query<User>().Where(x => x.Name == "ayende").ToArray();
				}
				Guid id;

				using (var session = store.OpenSession())
				{
					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
					session.Query<User>().Where(x => x.Name == "oren").Lazily();
					session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

				}
				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result, Default.Converters);
				Assert.Equal(304, responses[0].Status);
				Assert.Contains("oren", responses[0].Result.ToString());

				Assert.Equal(304, responses[1].Status);
				Assert.Contains("ayende", responses[1].Result.ToString());
			}
		}


		[Fact]
		public void CanProfilePartiallyAggressivelyCached()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Conventions.DisableProfiling = false; 
				store.Initialize();
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Load<User>("users/1");
					}
				}
				Guid id;

				using (var session = store.OpenSession())
				{
					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Advanced.Lazily.Load<User>("users/1");
						session.Advanced.Lazily.Load<User>("users/2");

						session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					}

				}
				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result, Default.Converters);
				Assert.Equal(0, responses[0].Status);
				Assert.Contains("oren", responses[0].Result.ToString());

				Assert.Equal(200, responses[1].Status);
				Assert.Contains("ayende", responses[1].Result.ToString());
			}
		}

		[Fact]
		public void CanProfileFullyAggressivelyCached()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Conventions.DisableProfiling = false; 
				store.Initialize();
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Load<User>("users/1");
						session.Load<User>("users/2");
					}
				}
				Guid id;

				using (var session = store.OpenSession())
				{
					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Advanced.Lazily.Load<User>("users/1");
						session.Advanced.Lazily.Load<User>("users/2");

						session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					}

				}
				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result, Default.Converters);
				Assert.Equal(0, responses[0].Status);
				Assert.Contains("oren", responses[0].Result.ToString());

				Assert.Equal(0, responses[1].Status);
				Assert.Contains("ayende", responses[1].Result.ToString());
			}
		}


		[Fact]
		public void CanProfileErrors()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Conventions.DisableProfiling = false; 
				store.Initialize();
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.SaveChanges();
				}


				Guid id;

				using (var session = store.OpenSession())
				{
					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
					session.Advanced.LuceneQuery<object, RavenDocumentsByEntityName>().WhereEquals("Not", "There").Lazily();
					Assert.Throws<InvalidOperationException>(() => session.Advanced.Eagerly.ExecuteAllPendingLazyOperations());
				}
				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result, Default.Converters);
				Assert.Equal(500, responses[0].Status);
				Assert.Contains("The field 'Not' is not indexed, cannot query on fields that are not indexed", responses[0].Result.ToString());
			}
		}
	}
}