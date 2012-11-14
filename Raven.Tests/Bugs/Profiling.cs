using System;
using Raven.Client.Document;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class Profiling : RemoteClientTest
	{
		[Fact]
		public void CanTrackLoadActions()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url =  "http://localhost:8079"})
			{
				store.InitializeProfiling();
				store.Initialize();
				// make the replication check here
				using(var session = store.OpenSession())
				{
					session.Load<User>("users/1");
				}

				Guid id;
				using (var session = store.OpenSession())
				{
					session.Load<User>("users/1");

					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
				}

				var profilingInformation = store.GetProfilingInformationFor(id);

				Assert.Equal(1, profilingInformation.Requests.Count);
			}
		}

		[Fact]
		public void CanTrackQueries()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"})
			{
				store.InitializeProfiling();
				store.Initialize();
				// make the replication check here
				using (var session = store.OpenSession())
				{
					session.Load<User>("users/1");
				}

				Guid id;
				using (var session = store.OpenSession())
				{
					session.Query<User>().ToList();

					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
				}

				var profilingInformation = store.GetProfilingInformationFor(id);

				Assert.Equal(1, profilingInformation.Requests.Count);
			}

		}

		[Fact]
		public void CanTrackPosts()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.InitializeProfiling();
				store.Initialize();
				
				// make hilo & replication checks here
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				Guid id;
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();

					id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;
				}

				var profilingInformation = store.GetProfilingInformationFor(id);

				
				Assert.Equal(1, profilingInformation.Requests.Count);
			}
		}
	}
}