using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList.PhilJones
{
	public class PhilJones88
	{
		public class Admin
		{
			public string Id { get; set; }
			public string Username { get; set; }
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Username { get; set; }
			public string Name { get; set; }
		}

		public class Proposal
		{
			public string Id { get; set; }
			public string AdminUserId { get; set; }
			public string CustomerUserId { get; set; }

			public string LocationName { get; set; }
			public string TripName { get; set; }
			public DateTime? DepartureDate { get; set; }

			public bool HasBeenSent { get; set; }
			public bool IsFromTailorMade { get; set; }

			public DateTime Created { get; set; }
		}

		public class ProposalListSearch
		{
			public string[] ProposalIds { get; set; }
			public string CustomerUserId { get; set; }
			public object[] Query { get; set; }
			public DateTime Created { get; set; }
		}

		public class ProposalListSearchResult
		{
			public string Id { get; set; }
			public string TripName { get; set; }
			public DateTime? DepartureDate { get; set; }
			public bool HasBeenSent { get; set; }
			public bool IsFromTailorMade { get; set; }
			public string CustomerName { get; set; }
			public string SalesPerson { get; set; }
			public DateTime Created { get; set; }
		}

		public class Proposals_ListProjection : AbstractMultiMapIndexCreationTask<ProposalListSearch>
		{
			public Proposals_ListProjection()
			{
				AddMap<Customer>(customers => from customer in customers
											  select new
											  {
												  ProposalIds = new string[0],
												  CustomerUserId = customer.Id,
												  Query = new object[] { customer.Username, customer.Name },
												  Created = DateTime.MinValue
											  });

				AddMap<Proposal>(proposals => from proposal in proposals
											  let departure = proposal.DepartureDate ?? DateTime.MinValue
											  select new
											  {
												  ProposalIds = new[] { proposal.Id },
												  proposal.CustomerUserId,
												  Query = new object[]
				                              	{
				                              		proposal.LocationName,
				                              		departure.ToString("MMM yyyy"),
				                              		departure.ToString("dd/MM/yyyy")
				                              	},
												  proposal.Created
											  });

				Reduce = results => from result in results
									group result by result.CustomerUserId
										into g
										select new
										{
											ProposalIds = g.SelectMany(x => x.ProposalIds).ToArray(),
											CustomerUserId = g.Key,
											Query = g.SelectMany(x => x.Query).ToArray(),
											Created = g.Max(x => (DateTime)x.Created)
										};

				Index(x => x.Query, FieldIndexing.Analyzed);
				Store(x => x.Query, FieldStorage.No);

				TransformResults = (database, results) => from result in results
														  let customer = database.Load<Customer>(result.CustomerUserId)
														  from proposalId in result.ProposalIds.DefaultIfEmpty()
														  let proposal = database.Load<Proposal>(proposalId)
														  let admin = database.Load<Admin>(proposal.AdminUserId)
														  orderby proposal.Created descending 
														  select
														  new
														  {
															  proposal.Id,
															  TripName = proposal.TripName,
															  proposal.DepartureDate,
															  proposal.HasBeenSent,
															  proposal.IsFromTailorMade,
															  CustomerName = customer.Name,
															  SalesPerson = admin.Username,
															  proposal.Created
														  };
			}
		}

		[Fact]
		public void OrderByDescending_is_ignored_when_using_multimap_index()
		{
			using (var store = new EmbeddableDocumentStore
			{
				Configuration =
				{
					RunInMemory = true
				},
				Conventions =
				{
					DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites
				}
			}.Initialize())
			{
				new Proposals_ListProjection().Execute(store);

				using (var session = store.OpenSession())
				{
					var admin = new Admin { Username = "Admin" };

					var customer = new Customer { Username = "mike", Name = "mike smith" };

					session.Store(admin);
					session.Store(customer);
					session.SaveChanges();

					var p1 = new Proposal { AdminUserId = admin.Id, CustomerUserId = customer.Id, TripName = "A", LocationName = "B", Created = new DateTime(2012, 2, 13) };
					var p2 = new Proposal { AdminUserId = admin.Id, CustomerUserId = customer.Id, TripName = "B", LocationName = "C", Created = new DateTime(2012, 2, 15) };
					var p3 = new Proposal { AdminUserId = admin.Id, CustomerUserId = customer.Id, TripName = "B", LocationName = "C", Created = new DateTime(2012, 2, 16) };
					var p4 = new Proposal { AdminUserId = admin.Id, CustomerUserId = customer.Id, TripName = "B", LocationName = "C", Created = new DateTime(2012, 2, 17) };
					var p5 = new Proposal { AdminUserId = admin.Id, CustomerUserId = customer.Id, TripName = "B", LocationName = "C", Created = new DateTime(2012, 1, 16) };
					var p6 = new Proposal { AdminUserId = admin.Id, CustomerUserId = customer.Id, TripName = "B", LocationName = "C", Created = new DateTime(2012, 1, 1) };

					session.Store(p1);
					session.Store(p2);
					session.Store(p3);
					session.Store(p4);
					session.Store(p5);
					session.Store(p6);
					session.SaveChanges();

					var proposals = session.Query<ProposalListSearch, Proposals_ListProjection>()
											.Customize(x => x.WaitForNonStaleResultsAsOfNow()) // strangely required to actually get data back, actual system just uses QueryYourWrites not this
											.As<ProposalListSearchResult>()
											.Take(10)
											.ToList();

					Assert.Equal(6, proposals.Count());
					Assert.Equal(new DateTime(2012, 2, 17), proposals[0].Created);
					Assert.Equal(new DateTime(2012, 2, 16), proposals[1].Created);
					Assert.Equal(new DateTime(2012, 2, 15), proposals[2].Created);
					Assert.Equal(new DateTime(2012, 2, 13), proposals[3].Created);
					Assert.Equal(new DateTime(2012, 1, 16), proposals[4].Created);
					Assert.Equal(new DateTime(2012, 1, 1), proposals[5].Created);
				}
			}
		}
	}
}