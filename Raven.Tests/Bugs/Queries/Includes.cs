//-----------------------------------------------------------------------
// <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class Includes : RavenTest
	{
		[Fact]
		public void IncludeWithDistinct()
		{
			using (var store = NewDocumentStore())
			{
				new MyCustIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Customer
					{
						Location = "PT CT",
						Occupation = "Marketing",
						CustomerId = "1",
						HeadingId = "2"
					}, "Customers/1");


					session.Store(new Customer
					{
						Location = "PT CT",
						Occupation = "Marketing",
						CustomerId = "1",
						HeadingId = "2"
					}, "Customers/2");
					session.SaveChanges();
				}

				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{

					RavenQueryStatistics qs;
					var qRes = session.Advanced.DocumentQuery<Customer>("MyCustIndex")
						.Statistics(out qs).Where("Occupation:Marketing")
						.Distinct()
						.SelectFields<CustomerHeader>("CustomerId")
						.Take(20)
						.Include("__document_id")
						.ToList();
					
					Assert.Equal(1, qRes.Count);
					var cust = session.Load<Customer>(qRes[0].Id);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void IncludeWithDistinctAndTransformer()
		{
			using (var store = NewDocumentStore())
			{
				new MyCustIndex().Execute(store);
				new IdentityTransformer().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Customer
					{
						Location = "PT CT",
						Occupation = "Marketing",
						CustomerId = "1",
						HeadingId = "2"
					}, "Customers/1");


					session.Store(new Customer
					{
						Location = "PT CT",
						Occupation = "Marketing",
						CustomerId = "1",
						HeadingId = "2"
					}, "Customers/2");
					session.SaveChanges();
				}

				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{

					RavenQueryStatistics qs;
					var qRes = session.Advanced.DocumentQuery<Customer>("MyCustIndex")
						.Statistics(out qs).Where("Occupation:Marketing")
						.Distinct()
						.SelectFields<Customer>("CustomerId")
						.Take(20)
						.SetResultTransformer(new IdentityTransformer().TransformerName)
						.ToList();

					WaitForUserToContinueTheTest(store);

					Assert.Equal(1, qRes.Count);
					Assert.Equal("Customers/1", qRes[0].Id);
					Assert.Equal("PT CT", qRes[0].Location);
				}
			}
		}


		public class CustomerHeader
		{
			public string CustomerId { get; set; }
			public string Id { get; set; }
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Location { get; set; }
			public string Occupation { get; set; }
			public string CustomerId { get; set; }
			public string HeadingId { get; set; }
		}

		public class IdentityTransformer : AbstractTransformerCreationTask<Customer>
		{
			public IdentityTransformer()
			{
				TransformResults = customers =>
					from customer in customers
					select LoadDocument<Customer>(customer.Id);
			}
		}

		public class MyCustIndex : AbstractIndexCreationTask<Customer>
		{
			public MyCustIndex()
			{
				Map = customers => from customer in customers
								   select new { customer.Occupation, customer.CustomerId };
				Store(x => x.Occupation, FieldStorage.Yes);
				Store(x => x.CustomerId, FieldStorage.Yes);
			}
		}
		[Fact]
		public void CanIncludeViaNestedPath()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User
					{
						Name = "Rahien",
						Friends = new[]
						{
							new DenormalizedReference {Name = "Ayende", Id = "users/1"},
						}
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Include<User>(x => x.Friends.Select(f => f.Id)).Load<User>("users/2");
					Assert.Equal(1, user.Friends.Length);
					foreach (var denormalizedReference in user.Friends)
					{
						s.Load<User>(denormalizedReference.Id);
					}

					Assert.Equal(1, s.Advanced.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void CanGenerateComplexPaths()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User
					{
						Name = "Rahien",
						Friends = new[]
						{
							new DenormalizedReference {Name = "Ayende", Id = "users/1"},
						}
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Include("Friends,Id").Load<User>("users/2");
					Assert.Equal(1, user.Friends.Length);
					foreach (var denormalizedReference in user.Friends)
					{
						s.Load<User>(denormalizedReference.Id);
					}

					Assert.Equal(1, s.Advanced.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void CanIncludeViaLinq()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new Person { Name = "Stuart", PhoneNumber = "55567453" });
					s.Store(new User
					{
						Name = "Piers",
						EmergencyPerson = new EmergencyContact { PersonId = 1, Relationship = "Father" }
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Include<User, Person>(u => u.EmergencyPerson.PersonId).Load<User>("users/1");
					Assert.NotNull(user.EmergencyPerson);

					// Should be loaded from cache
					var emergencyContact = s.Load<Person>(user.EmergencyPerson.PersonId);
					Assert.NotNull(emergencyContact);

					Assert.Equal(1, s.Advanced.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void CanIncludeViaLinqWithValueTypeId()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					var personId = Guid.NewGuid();

					s.Store(new PersonGuid { Id = personId, Name = "Stuart", PhoneNumber = "55567453" });
					s.Store(new User
					{
						Name = "Piers",
						EmergencyPersonByGuid = new EmergencyContactGuid { PersonId = personId, Relationship = "Father" }
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Include<User, PersonGuid>(u => u.EmergencyPersonByGuid.PersonId).Load<User>("users/1");
					Assert.NotNull(user.EmergencyPersonByGuid);

					// Should be loaded from cache
					var emergencyContact = s.Load<PersonGuid>(user.EmergencyPersonByGuid.PersonId);
					Assert.NotNull(emergencyContact);

					Assert.Equal(1, s.Advanced.NumberOfRequests);
				}
			}
		}

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public DenormalizedReference[] Friends { get; set; }
			public EmergencyContact EmergencyPerson { get; set; }
			public EmergencyContactGuid EmergencyPersonByGuid { get; set; }
		}

		public class DenormalizedReference
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Person
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string PhoneNumber { get; set; }
		}

		public class EmergencyContact
		{
			public int PersonId { get; set; }
			public string Relationship { get; set; }
		}

		public class PersonGuid
		{
			public Guid Id { get; set; }
			public string Name { get; set; }
			public string PhoneNumber { get; set; }
		}

		public class EmergencyContactGuid
		{
			public Guid PersonId { get; set; }
			public string Relationship { get; set; }
		}
	}
}
