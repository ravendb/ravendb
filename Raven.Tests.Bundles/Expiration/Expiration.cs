//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Bundles.Versioning;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bundles.Expiration
{
	public class Expiration : RavenTest
	{
		private readonly DocumentStore documentStore;
		private readonly RavenDbServer ravenDbServer;

		public Expiration()
		{
			SystemTime.UtcDateTime = () => DateTime.UtcNow;
            ravenDbServer = GetNewServer(activeBundles: "DocumentExpiration", configureConfig: configuration =>
			{
				configuration.Settings["Raven/Expiration/DeleteFrequencySeconds"] = "1";
			});
			documentStore = NewRemoteDocumentStore(ravenDbServer: ravenDbServer);
		}

		public override void Dispose()
		{
			SystemTime.UtcDateTime = null;
			base.Dispose();
		}

		[Fact]
		public void Can_add_entity_with_expiry_then_read_it_before_it_expires()
		{
			var company = new Company {Name = "Company Name"};
			var expiry = SystemTime.UtcNow.AddMinutes(5);
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.Advanced.GetMetadataFor(company)["Raven-Expiration-Date"] = new RavenJValue(expiry.ToString(Default.DateTimeOffsetFormatsToWrite));
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var company2 = session.Load<Company>(company.Id);
				Assert.NotNull(company2);
				var metadata = session.Advanced.GetMetadataFor(company2);
				var expirationDate = metadata["Raven-Expiration-Date"];
				Assert.NotNull(expirationDate);
				var dateTime = expirationDate.Value<DateTime>();
				Assert.Equal(DateTimeKind.Utc, dateTime.Kind);
				Assert.Equal(expiry, expirationDate);
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
			SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
		   
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

                session.Advanced.DocumentQuery<Company>("Raven/DocumentsByExpirationDate")
					.WaitForNonStaleResults()
					.ToList();
			}
			SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

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
			for (var i = 0; i < 100; i++)
			{
				ravenDbServer.SystemDatabase.TransactionalStorage.Batch(accessor =>
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