using System;
using System.Linq;
using LinQTests;
using Raven.Client;
using Raven.Client.Document;
using Raven.Server;
using Xunit;

namespace Raven.Tests.MailingList.Thor
{
	public class LinqTest : RavenTest, IDisposable
	{
		private readonly RavenDbServer ravenDbServer;
		private readonly IDocumentStore documentStore;

		public LinqTest()
		{
			ravenDbServer = GetNewServer();

			documentStore = new DocumentStore
			           	{
			           		Url = "http://localhost:8079",
			           		Conventions = {DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites}
			           	}.Initialize();
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			base.Dispose();
		}

		[Fact]
		public void SingleTransportWithoutChildren_Raven()
		{
			// Create Index
			new TransportsIndex().Execute(documentStore);

			using (var session = documentStore.OpenSession())
			{
				// Store a single Transport
				session.Store(new Transport { Id = "A1", ChildId = "Test" });

				session.SaveChanges();

				var query = session.Query<JoinedChildTransport, TransportsIndex>().Customize(x => x.WaitForNonStaleResultsAsOfNow())
					//                 .AsProjection<JoinedChildTransport>()
					;

				var transports = query.ToList();
				Assert.Equal(1, transports.Count);

				Assert.Equal("A1", transports[0].TransportId);
				Assert.Equal("Test", transports[0].ChildId);
				Assert.Null(transports[0].Name);
			}
		}

		[Fact]
		public void SingleChildAndNoTransport_Raven()
		{
			// Create Index
			new TransportsIndex().Execute(documentStore);

			using (var session = documentStore.OpenSession())
			{
				// Store a single Transport
				session.Store(new Child { Id = "B1", Name = "Thor Arne" });

				session.SaveChanges();

				var query = session.Query<JoinedChildTransport, TransportsIndex>().Customize(x => x.WaitForNonStaleResultsAsOfNow())
					//                 .AsProjection<JoinedChildTransport>()
					;

				var transports = query.ToList();
				Assert.Equal(1, transports.Count);  // Also check for indexing error
			}
		}

		[Fact]
		public void MultipleChildrenWithMultipleTransports_Raven()
		{
			// Create Index
			new TransportsIndex().Execute(documentStore);

			using (var session = documentStore.OpenSession())
			{
				// Store two children
				session.Store(new Child { Id = "B1", Name = "Thor Arne" });
				session.Store(new Child { Id = "B2", Name = "Ståle" });

				// Store four Transports
				session.Store(new Transport { Id = "A1", ChildId = "B1" });
				session.Store(new Transport { Id = "A2", ChildId = "B1" });
				session.Store(new Transport { Id = "A3", ChildId = "B2" });
				session.Store(new Transport { Id = "A4", ChildId = "B2" });

				session.SaveChanges();

				var query = session.Query<JoinedChildTransport, TransportsIndex>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.OrderBy(x=>x.TransportId)
					//                 .AsProjection<JoinedChildTransport>()
					;

				var transports = query.ToList();
				Assert.Equal(4, transports.Count);

				// The test below may have to change to accound for unpredictable order, but we never even get the correct number of hits

				// transports for B1
				Assert.Equal("A1", transports[0].TransportId);
				Assert.Equal("B1", transports[0].ChildId);
				Assert.Equal("Thor Arne", transports[0].Name);

				Assert.Equal("A2", transports[1].TransportId);
				Assert.Equal("B1", transports[1].ChildId);
				Assert.Equal("Thor Arne", transports[1].Name);

				// transports for B2
				Assert.Equal("A3", transports[2].TransportId);
				Assert.Equal("B2", transports[2].ChildId);
				Assert.Equal("Ståle", transports[2].Name);

				Assert.Equal("A4", transports[3].TransportId);
				Assert.Equal("B2", transports[3].ChildId);
				Assert.Equal("Ståle", transports[3].Name);
			}
		}
	}
}