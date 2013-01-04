using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList.spokeypokey
{
	public class Spokey : RavenTest
	{

		private class Employee
		{
			public string FirstName { get; set; }
			public string[] ZipCodes { get; set; }
			public List<string> ZipCodes2 { get; set; }
		}

		[Fact]
		public void Can_query_empty_list()
		{
			var user1 = new Employee() {FirstName = "Joe", ZipCodes2 = new List<string>()};
			var length = user1.ZipCodes2.Count;
			Assert.Equal(0, length);
			using (var docStore = NewDocumentStore())
			{
				using (var session = docStore.OpenSession())
				{

					session.Store(user1);
					session.SaveChanges();
				}
				using (var session = docStore.OpenSession())
				{

					var result = (from u in session.Query<Employee>().Customize(x => x.WaitForNonStaleResults())
								  where u.ZipCodes2.Count == 0
								  select u).ToArray();

					Assert.Empty(docStore.DocumentDatabase.Statistics.Errors);
					Assert.Equal(1, result.Count());
				}
			}
		}
		[Fact]
		public void Can_query_empty_array()
		{
			var user1 = new Employee() { FirstName = "Joe", ZipCodes = new string[] { } };
			var length = user1.ZipCodes.Length;
			Assert.Equal(0, length);
			using(var docStore = NewDocumentStore())
			{
				using (var session = docStore.OpenSession())
				{

					session.Store(user1);
					session.SaveChanges();
				}
				using (var session = docStore.OpenSession())
				{
					var result = (from u in session.Query<Employee>().Customize(x=>x.WaitForNonStaleResults())
								  where u.ZipCodes.Length == 0
								  select u).ToArray();

					Assert.Empty(docStore.DocumentDatabase.Statistics.Errors);
					Assert.Equal(1, result.Count());
				}
			}
		}
		public class Reference
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
		}


		public class TaxonomyCode : Reference
		{
			public string Code { get; set; }
			public DateTime EffectiveFrom { get; set; }
			public DateTime EffectiveThrough { get; set; }
		}

		public class Provider1
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
			public Reference TaxonomyCodeRef { get; set; }
		}

		public class ProviderTestDto
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
			public TaxonomyCode TaxonomyCode { get; set; }
		}

		public class IdentityProjectionIndex1 : AbstractIndexCreationTask<Provider1>
		{
			public IdentityProjectionIndex1()
			{
				Map =
					providers => from provider in providers
								 select new
								 {
									 provider.InternalId,
									 provider.Name,
								 };

				TransformResults =
					(db, providers) => from provider in providers
									   let TaxonomyCode = db.Load<TaxonomyCode>(provider.TaxonomyCodeRef.InternalId)
									   select new
									   {
										   provider.InternalId,
										   provider.Name,
										   TaxonomyCode,
									   };
			}
		}

		[Fact]
		public void Can_project_InternalId_from_transformResults2()
		{
			var taxonomyCode1 = new TaxonomyCode
			{
				EffectiveFrom = new DateTime(2011, 1, 1),
				EffectiveThrough = new DateTime(2011, 2, 1),
				InternalId = "taxonomycodetests/1",
				Name = "ANESTHESIOLOGY",
				Code = "207L00000X",
			};
			var provider1 = new Provider1
			{
				Name = "Joe Schmoe",
				TaxonomyCodeRef = new Reference
				{
					InternalId = taxonomyCode1.InternalId,
					Name = taxonomyCode1.Name
				}
			};

			using(GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				store.Conventions.FindIdentityProperty = (x => x.Name == "InternalId");

				using (var session = store.OpenSession())
				{
					session.Store(taxonomyCode1);
					session.Store(provider1);
					session.SaveChanges();
				}

				new IdentityProjectionIndex1().Execute(store);
				using (var session = store.OpenSession())
				{
					var result = (from p in session.Query<Provider1, IdentityProjectionIndex1>()
								  .Customize(x => x.WaitForNonStaleResults())
								  select p).First();
					Assert.Equal(provider1.Name, result.Name);
					Assert.Equal(provider1.InternalId, result.InternalId);
				}
			}
		}
	}
}