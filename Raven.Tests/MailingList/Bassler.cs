// -----------------------------------------------------------------------
//  <copyright file="Bassler .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList
{
	public class Bassler : RavenTest
	{
		[Fact]
		public void Test()
		{
			using (var store = NewDocumentStore())
			{
				new App_WaiverWaitlistItemSearch().Execute(store);
				using (var session = store.OpenSession())
				{

					var wwle1 = new WaiverWaitlistItem("clients/1", DateTime.Today, "5") { Id = "waiverwaitlistitems/1" };
					var wwle2 = new WaiverWaitlistItem("clients/1", DateTime.Today.AddDays(7), "1") { Id = "waiverwaitlistitems/2" };
					session.Store(wwle1);
					session.Store(wwle2);

					var c1 = new TestClient { ClientProfile = { PersonName = new TestPersonName("John", "Able") }, Id = "clients/1" };
					var c2 = new TestClient { ClientProfile = { PersonName = new TestPersonName("Joe", "Cain") }, Id = "clients/2" };

					session.Store(c1);
					session.Store(c2);


					session.SaveChanges();

					var list = session.Query<App_WaiverWaitlistItemSearch.IndexResult, App_WaiverWaitlistItemSearch>()
						.AsProjection<App_WaiverWaitlistItemSearch.IndexResult>()
						.ToList();
					Assert.False(list.Any(result => result.GroupNumber == null));
					Assert.False(list.Any(result => result.ScreeningDate == DateTime.MinValue));
					Assert.False(list.Any(result => result.LastName == null));
				}
			}
		}
		public class WaiverWaitlistItem
		{
			public WaiverWaitlistItem()
			{
			}

			public WaiverWaitlistItem(string clientId, DateTime? screeningDate, string groupNumber)
			{
				ClientId = clientId;
				ScreeningDate = screeningDate;
				GroupNumber = groupNumber;
			}

			public string Id { get; set; }
			public DateTime? ScreeningDate { get; set; }
			public string ClientId { get; set; }
			public string GroupNumber { get; set; }
		}

		public class TestClient
		{
			public TestClient()
			{
				ClientProfile = new ClientProfile();
			}

			public string Id { get; set; }
			public ClientProfile ClientProfile { get; set; }
		}

		public class ClientProfile
		{
			public TestPersonName PersonName { get; set; }
		}

		public class TestPersonName
		{
			public TestPersonName()
			{
			}

			public TestPersonName(string firstName, string lastName)
				: this(firstName, null, lastName, null)
			{
			}

			public TestPersonName(string firstName, string middleName, string lastName)
				: this(firstName, middleName, lastName, null)
			{
			}

			public TestPersonName(string firstName, string middleName, string lastName, string suffix)
			{
				FirstName = firstName;
				MiddleName = middleName;
				LastName = lastName;
				Suffix = suffix;
			}

			public string FirstName { get; private set; }
			public string MiddleName { get; private set; }
			public string LastName { get; private set; }
			public string Suffix { get; private set; }
		}

		public class App_WaiverWaitlistItemSearch : AbstractMultiMapIndexCreationTask<App_WaiverWaitlistItemSearch.IndexResult>
		{
			public class IndexResult
			{
				public string Id { get; set; }
				public DateTime? ScreeningDate { get; set; }
				public string ClientId { get; set; }
				public string LastName { get; set; }
				public string GroupNumber { get; set; }
			}

			public App_WaiverWaitlistItemSearch()
			{
				AddMap<WaiverWaitlistItem>(waiverWaitlistEntries => from waitlistEntry in waiverWaitlistEntries
																	select new
																	{
																		Id = waitlistEntry.Id,
																		ScreeningDate = waitlistEntry.ScreeningDate,
																		ClientId = waitlistEntry.ClientId,
																		LastName = (string)null,
																		GroupNumber = waitlistEntry.GroupNumber,
																	}
							 );

				AddMap<TestClient>(clients => from client in clients
											  select new
											  {
												  Id = client.Id,
												  ScreeningDate = DateTime.MinValue,
												  ClientId = client.Id,
												  LastName = client.ClientProfile.PersonName.LastName,
												  GroupNumber = (string)null,
											  }
							   );



				Reduce = results =>
					from result in results
					group result by result.ClientId into g
					let lastName = g.FirstOrDefault(x => x.LastName != null).LastName
					let groupNumber = g.FirstOrDefault(x => x.GroupNumber != null).GroupNumber
					let screeningDate = g.FirstOrDefault(x => x.ScreeningDate != DateTime.MinValue).ScreeningDate
					from item in g
					select new
					{
						Id = item.Id,
						ScreeningDate = screeningDate,
						ClientId = g.Key,
						GroupNumber = groupNumber,
						LastName = lastName,
					};

				Store(ir => ir.ScreeningDate, FieldStorage.Yes);
				Store(ir => ir.ClientId, FieldStorage.Yes);
				Store(ir => ir.LastName, FieldStorage.Yes);
				Store(ir => ir.GroupNumber, FieldStorage.Yes);

				Indexes.Add(ir => ir.ScreeningDate, FieldIndexing.Analyzed);
				Indexes.Add(ir => ir.ClientId, FieldIndexing.Analyzed);
				Indexes.Add(ir => ir.LastName, FieldIndexing.Analyzed);
				Indexes.Add(ir => ir.GroupNumber, FieldIndexing.Analyzed);
			}
		}


	}
}