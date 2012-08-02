//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Bundles.Tests.Versioning;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;
using System.Linq;

namespace Raven.Bundles.Tests.Expiration
{
	public class Expiration : IDisposable
	{
		private readonly string path;
		private readonly DocumentStore documentStore;
		private readonly RavenDbServer ravenDbServer;

		public Expiration()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Versioning.Versioning)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
			var ravenConfiguration = new Raven.Database.Config.RavenConfiguration
			{
				Port = 8079,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				DataDirectory = path,
				Settings =
					{
						{"Raven/Expiration/DeleteFrequencySeconds", "1"},
						{"Raven/ActiveBundles", "Expiration"}
					}
			};
			ravenConfiguration.PostInit();
			ravenDbServer = new RavenDbServer(
				ravenConfiguration);
			Raven.Bundles.Expiration.ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow;
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
		}

		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}

		[Fact]
		public void Can_add_entity_with_expiry_then_read_it_before_it_expires()
		{
			var company = new Company {Name = "Company Name"};
			var expiry = DateTime.UtcNow.AddMinutes(5);
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.Advanced.GetMetadataFor(company)["Raven-Expiration-Date"] = new RavenJValue(expiry);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var company2 = session.Load<Company>(company.Id);
				Assert.NotNull(company2);
				var metadata = session.Advanced.GetMetadataFor(company2);
				var expirationDate = metadata.Value<DateTime>("Raven-Expiration-Date");
				Assert.Equal(DateTimeKind.Utc, expirationDate.Kind);
				Assert.Equal(expiry.ToString(), expirationDate.ToString());
			}
		}

		[Fact]
		public void Can_add_entity_with_expiry_but_will_not_be_able_to_read_it_after_expiry()
		{
			var company = new Company { Name = "Company Name" };
			var expiry = DateTime.UtcNow.AddMinutes(5);
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.Advanced.GetMetadataFor(company)["Raven-Expiration-Date"] = new RavenJValue(expiry);
				session.SaveChanges();
			}
			Raven.Bundles.Expiration.ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow.AddMinutes(10);
		   
			using (var session = documentStore.OpenSession())
			{
				var company2 = session.Load<Company>(company.Id);
				Assert.Null(company2);
			}
		}

		[Fact]
		public void After_expiry_passed_document_will_be_physically_deleted()
		{
			var company = new Company
			{
				Id = "companies/1",
				Name = "Company Name"
			};
			var expiry = DateTime.UtcNow.AddMinutes(5);
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.Advanced.GetMetadataFor(company)["Raven-Expiration-Date"] = new RavenJValue(expiry);
				session.SaveChanges();

				session.Advanced.LuceneQuery<Company>("Raven/DocumentsByExpirationDate")
					.WaitForNonStaleResults()
					.ToList();
			}
			Raven.Bundles.Expiration.ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow.AddMinutes(10);

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company
				{
					Id = "companies/2",
					Name = "Company Name"
				});
				session.SaveChanges(); // this forces the background task to run
			}

			JsonDocument documentByKey = null;
			for (int i = 0; i < 100; i++)
			{
				ravenDbServer.Database.TransactionalStorage.Batch(accessor =>
				{
					documentByKey = accessor.Documents.DocumentByKey("companies/1", null);
				});
				if (documentByKey == null)
					return;
				Thread.Sleep(100);
			}
			Assert.False(true, "Document was not deleted");
		}
	}
}