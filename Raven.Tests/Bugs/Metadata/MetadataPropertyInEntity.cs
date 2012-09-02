//-----------------------------------------------------------------------
// <copyright file="MetadataPropertyInEntity.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
	public class MetadataPropertyInEntity : RavenTest
	{
		public class Account
		{
			public string Id { get; set; }
			public long Revision { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void Can_use_entity_property_for_metadata()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var account = new Account
					{
						Name = "Hibernating Rhinos"
					};
					session.Store(account);
					session.Advanced.GetMetadataFor(account)["Raven-Document-Revision"] = 1;
					session.SaveChanges();
				}

				store.RegisterListener(new MetadataToPropertyConvertionListener());

				using (var session = store.OpenSession())
				{
					var account = session.Load<Account>("accounts/1");
					Assert.Equal(1, account.Revision);
				}
			}
		}

		public class MetadataToPropertyConvertionListener : IDocumentConversionListener
		{
			/// <summary>
			/// Called when converting an entity to a document and metadata
			/// </summary>
			public void EntityToDocument(object entity, RavenJObject document, RavenJObject metadata)
			{
				if (entity is Account == false)
					return;
				document.Remove("Revision");
			}

			/// <summary>
			/// Called when converting a document and metadata to an entity
			/// </summary>
			public void DocumentToEntity(object entity, RavenJObject document, RavenJObject metadata)
			{
				if (entity is Account == false)
					return;
				((Account)entity).Revision = metadata.Value<long>("Raven-Document-Revision");
			}
		}

		[Fact]
		public void Can_use_entity_property_for_metadata_with_listener()
		{
			using(var store = NewDocumentStore())
			{
				store.RegisterListener(new RavenDocumentRevisionMetadataToRevisionProperty());
				using(var session = store.OpenSession())
				{
					var account = new Account
					{
						Name = "Hibernating Rhinos"
					};
					session.Store(account);
					session.Advanced.GetMetadataFor(account)["Raven-Document-Revision"] = 1;
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var account = session.Load<Account>("accounts/1");
					account.Name = "Rampaging Rhinos";
					Assert.Equal(1, account.Revision);
					session.SaveChanges();
				}

				var jsonDocument = store.DatabaseCommands.Get("accounts/1");
				Assert.Null(jsonDocument.DataAsJson["Revision"]);
			}
		}

		public class RavenDocumentRevisionMetadataToRevisionProperty : IDocumentConversionListener
		{
			/// <summary>
			/// Called when converting an entity to a document and metadata
			/// </summary>
			public void EntityToDocument(object entity, RavenJObject document, RavenJObject metadata)
			{
				if (entity is Account == false)
					return;
				document.Remove("Revision");
			}

			/// <summary>
			/// Called when converting a document and metadata to an entity
			/// </summary>
			public void DocumentToEntity(object entity, RavenJObject document, RavenJObject metadata)
			{
				if (entity is Account == false)
					return;
				((Account)entity).Revision = metadata.Value<long>("Raven-Document-Revision");

			}
		}
	}
}
