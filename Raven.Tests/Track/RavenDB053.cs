using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Track
{
	public class JoinedChildTransport
	{
		public string ChildId { get; set; }
		public string TransportId { get; set; }
		public string EntityType { get; set; }
		public string Name { get; set; }
	}

	public class Child
	{
		public string Id { get; set; }
		public string Name { get; set; }
	}

	public class Transport
	{
		public string Id { get; set; }
		public string ChildId { get; set; }
	}

	public class LinqTest : RavenTest
	{
		[Fact]
		public void ChildrenHasMultipleTransports_Memory()
		{
			// Hardcoded output of map
			IList<JoinedChildTransport> mapResults = new List<JoinedChildTransport>
			{
				new JoinedChildTransport
				{
					ChildId = "B1",
					TransportId = null,
					EntityType = "Barn",
					Name = "Thor Arne"
				},
				new JoinedChildTransport
				{
					ChildId = "B1",
					TransportId = "A1",
					EntityType = "Skyss",
					Name = null
				},
				new JoinedChildTransport
				{
					ChildId = "B1",
					TransportId = "A2",
					EntityType = "Skyss",
					Name = null
				},
				new JoinedChildTransport
				{
					ChildId = "B2",
					TransportId = null,
					EntityType = "Barn",
					Name = "Ståle"
				},
				new JoinedChildTransport
				{
					ChildId = "B2",
					TransportId = "A3",
					EntityType = "Skyss",
					Name = null
				},
				new JoinedChildTransport
				{
					ChildId = "B2",
					TransportId = "A4",
					EntityType = "Skyss",
					Name = null
				}
			};

			// "index"
			var skyssavtaler = (from result in mapResults
			                    group result by result.ChildId
			                    into g
									from transport in g.Where(transport => transport.EntityType == "Skyss").DefaultIfEmpty(new JoinedChildTransport())
									from barn in g.Where(barn => barn.EntityType == "Barn").DefaultIfEmpty(new JoinedChildTransport())

									select new { transport.ChildId, transport.TransportId, barn.Name }).ToList();

			Assert.Equal(4, skyssavtaler.Count);

			// skyssavtaler for B1
			Assert.Equal("A1", skyssavtaler[0].TransportId);
			Assert.Equal("B1", skyssavtaler[0].ChildId);
			Assert.Equal("Thor Arne", skyssavtaler[0].Name);

			Assert.Equal("A2", skyssavtaler[1].TransportId);
			Assert.Equal("B1", skyssavtaler[1].ChildId);
			Assert.Equal("Thor Arne", skyssavtaler[0].Name);

			// skyssavtaler for B2
			Assert.Equal("A3", skyssavtaler[2].TransportId);
			Assert.Equal("B2", skyssavtaler[2].ChildId);
			Assert.Equal("Ståle", skyssavtaler[2].Name);

			Assert.Equal("A4", skyssavtaler[3].TransportId);
			Assert.Equal("B2", skyssavtaler[3].ChildId);
			Assert.Equal("Ståle", skyssavtaler[3].Name);
		}



		public class TransportsIndex : AbstractMultiMapIndexCreationTask<JoinedChildTransport>
		{
			public TransportsIndex()
			{
				AddMap<Child>(childList => from child in childList
				                           select new
				                           {
				                           	ChildId = child.Id,
				                           	TransportId = (dynamic) null,
				                           	Name = child.Name,
				                           	EntityType = "Child"
				                           });

				AddMap<Transport>(transportList => from transport in transportList
				                                   select new
				                                   {
				                                   	ChildId = transport.ChildId,
				                                   	TransportId = transport.Id,
				                                   	Name = (dynamic) null,
				                                   	EntityType = "Transport"
				                                   });

				Reduce = results => from result in results
				                    group result by result.ChildId
				                    into g
										from transport in g.Where(transport=>transport.EntityType == "Transport").DefaultIfEmpty()
										from barn in g.Where(barn=>barn.EntityType == "Child").DefaultIfEmpty()
				                    select new {transport.EntityType, transport.ChildId, transport.TransportId, transport.Name};

				Store(x => x.ChildId, FieldStorage.Yes);
				Store(x => x.TransportId, FieldStorage.Yes);
				Store(x => x.Name, FieldStorage.Yes);
				Store(x => x.EntityType, FieldStorage.Yes);
			}
		}

		[Fact]
		public void ChildrenHasMultipleTransports_Raven()
		{
			using (var docStore = NewDocumentStore())
			{
				// Create Index
				new TransportsIndex().Execute(docStore);

				using (var session = docStore.OpenSession())
				{

					// Store two children
					session.Store(new Child {Id = "B1", Name = "Thor Arne"});
					session.Store(new Child {Id = "B2", Name = "Ståle"});

					// Store four Transports
					session.Store(new Transport {Id = "A1", ChildId = "B1"});
					session.Store(new Transport {Id = "A2", ChildId = "B1"});
					session.Store(new Transport {Id = "A3", ChildId = "B2"});
					session.Store(new Transport {Id = "A4", ChildId = "B2"});

					session.SaveChanges();

					var transports = session.Query<JoinedChildTransport, TransportsIndex>()
						.Customize(x=>x.WaitForNonStaleResults(TimeSpan.FromMinutes(100)))
						.AsProjection<JoinedChildTransport>()
						.ToList();

					Assert.Empty(docStore.DocumentDatabase.Statistics.Errors);

					Assert.Equal(4, transports.Count);

					// The test below may have to change to accound for unpredictable order, but we never even get the correct number of hits

					// skyssavtaler for B1
					Assert.Equal("A1", transports[0].TransportId);
					Assert.Equal("B1", transports[0].ChildId);
					Assert.Equal("Thor Arne", transports[0].Name);

					Assert.Equal("A2", transports[1].TransportId);
					Assert.Equal("B1", transports[1].ChildId);
					Assert.Equal("Thor Arne", transports[0].Name);

					// skyssavtaler for B2
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
}