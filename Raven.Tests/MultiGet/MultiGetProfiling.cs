using System;
using System.Linq;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client.Debug;
using Raven.Client.Linq;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetProfiling : RemoteClientTest
	{
		[Fact]
		public void CanProfileLazyRequests()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" })
			{
				store.Initialize();
				using (var session = store.OpenSession())
				{
					// handle the initial request for replication information
				}
				Guid id;
				using (var session = store.OpenSession())
				{
					id = session.Advanced.DatabaseCommands.ProfilingInformation.Id;
					session.Advanced.Lazily.Load<User>("users/1");
					session.Advanced.Lazily.Load<User>("users/2");
					session.Advanced.Lazily.Load<User>("users/3");

					session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
				}

				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result);
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
			using (var store = new DocumentStore { Url = "http://localhost:8080" })
			{
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
				}
				Guid id;
				
				using (var session = store.OpenSession())
				{
					id = session.Advanced.DatabaseCommands.ProfilingInformation.Id;
					session.Query<User>().Where(x => x.Name == "oren").Lazily();
					session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

				}
				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

				var responses = JsonConvert.DeserializeObject<GetResponse[]>(profilingInformation.Requests[0].Result);
				Assert.Equal(304, responses[0].Status);
				Assert.Contains("oren", responses[0].Result);
			}
		}
	}
}