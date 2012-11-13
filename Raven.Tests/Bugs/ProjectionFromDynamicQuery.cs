//-----------------------------------------------------------------------
// <copyright file="ProjectionFromDynamicQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class ProjectionFromDynamicQuery : RavenTest
	{
		[Fact]
		public void ProjectNameFromDynamicQueryUsingLucene()
		{
			using(var documentStore = NewDocumentStore())
			{
				using(var s = documentStore.OpenSession())
				{
					s.Store(new User{Name = "Ayende", Email = "Ayende@ayende.com"});
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				{
					var result = s.Advanced.LuceneQuery<User>()
						.WhereEquals("Name", "Ayende", isAnalyzed: true)
						.SelectFields<RavenJObject>("Email")
						.First();

					Assert.Equal("Ayende@ayende.com", result.Value<string>("Email"));
				}
			}
		}

		[Fact]
		public void ProjectNameFromDynamicQueryUsingLinq()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var s = documentStore.OpenSession())
				{
					s.Store(new User { Name = "Ayende", Email = "Ayende@ayende.com" });
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				{
					var result = from user in s.Query<User>()
								 where user.Name == "Ayende"
								 select new { user.Email };

					Assert.Equal("Ayende@ayende.com", result.First().Email);
				}
			}
		}

		[Fact]
		public void ProjectNameFromDynamicQueryUsingLuceneUsingNestedObject()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var s = documentStore.OpenSession())
				{
					s.Store(new Person()
					{
						Name = "Ayende",
						BillingAddress = new Address
						{
							City = "Bologna"
						}
					});
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				{
					var result = s.Advanced.LuceneQuery<Person>()
						.WhereEquals("Name", "Ayende", isAnalyzed: true)
						.SelectFields<RavenJObject>("BillingAddress")
						.First();

					Assert.Equal("Bologna", result.Value<RavenJObject>("BillingAddress").Value<string>("City"));
				}
			}
		}

		[Fact]
		public void ProjectNameFromDynamicQueryUsingLuceneUsingNestedProperty()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var s = documentStore.OpenSession())
				{
					s.Store(new Person()
					{
						Name = "Ayende",
						BillingAddress = new Address
						{
							City = "Bologna"
						}
					});
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				{
					var result = s.Advanced.LuceneQuery<Person>()
						.WhereEquals("Name", "Ayende", isAnalyzed: true)
						.SelectFields<RavenJObject>("BillingAddress.City")
						.First();

					Assert.Equal("Bologna", result.Value<string>("BillingAddress.City"));
				}
			}

		}

		[Fact]
		public void ProjectNameFromDynamicQueryUsingLuceneUsingNestedArray()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var s = documentStore.OpenSession())
				{
					s.Store(new Person()
					{
						Name = "Ayende",
						BillingAddress = new Address
						{
							City = "Bologna"
						},
						Addresses = new Address[]
						{
							new Address {City = "Old York"},
						}
					});
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				{
					var result = s.Advanced.LuceneQuery<Person>()
						.WhereEquals("Name", "Ayende", isAnalyzed: true)
						.SelectFields<RavenJObject>("Addresses[0].City")
						.First();

					Assert.Equal("Old York", result.Value<string>("Addresses[0].City"));
				}
			}
		}

		private class Person
		{
			public string Name { get; set; }
			public Address BillingAddress { get; set; }

			public Address[] Addresses { get; set; }
		}

		public class Address
		{
			public string City { get; set; }
		}
	}
}
