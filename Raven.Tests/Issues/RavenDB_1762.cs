// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1762.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1762 : ReplicationBase
	{
		private const string docId = "users/1";

		protected override void ConfigureServer(InMemoryRavenConfiguration serverConfiguration)
		{
			serverConfiguration.Catalog.Catalogs.Add(new TypeCatalog(typeof (DeleteOnConflict)));
		}

		private void CanResolveConflict(Action<DeleteOnConflict> alterResolver, Action<IDocumentStore, IDocumentStore> testCallback)
		{
			DocumentStore store1 = CreateStore();
			DocumentStore store2 = CreateStore();

			using (IDocumentSession session = store1.OpenSession())
			{
				session.Store(new User
				{
					Name = "Oren"
				}, docId);
				session.SaveChanges();
			}

			// master - master
			SetupReplication(store1.DatabaseCommands, store2.Url);

			WaitForReplication(store2, docId);

			// get reference to custom conflict resolver
			DeleteOnConflict resolver = DeleteOnConflict.Instance;
			Assert.NotNull(resolver);
			if (alterResolver != null)
			{
				alterResolver(resolver);
			}

			testCallback(store1, store2);
		}

		[Fact]
		public void CanResolveConflictBetweenPutAndDelete_IncomingDelete()
		{
			CanResolveConflict(resolver => resolver.Enabled = true, delegate(IDocumentStore store1, IDocumentStore store2)
			{
				using (IDocumentSession s = store2.OpenSession())
				{
					var user = s.Load<User>(docId);
					user.Active = true;
					s.Store(user);
					s.SaveChanges();
				}

				using (IDocumentSession s = store1.OpenSession())
				{
					var user = s.Load<User>(docId);
					s.Delete(user);
					s.SaveChanges();
				}

				WaitForReplication(store2, session => session.Load<User>(docId) == null);
				Assert.Null(store2.DatabaseCommands.Get(docId));
			}
				);
		}

		[Fact]
		public void CanResolveConflictBetweenPutAndDelete_IncomingPut()
		{
			CanResolveConflict(resolver => resolver.Enabled = true, delegate(IDocumentStore store1, IDocumentStore store2)
			{
				using (IDocumentSession s = store2.OpenSession())
				{
					var user = s.Load<User>(docId);
					s.Delete(user);
					s.SaveChanges();
				}
				using (IDocumentSession s = store1.OpenSession())
				{
					var user = s.Load<User>(docId);
					user.Active = true;
					s.Store(user);
					s.SaveChanges();
				}

				WaitForReplication(store2, session => session.Load<User>(docId) == null);
				Assert.Null(store2.DatabaseCommands.Get(docId));
			}
				);
		}

		[Fact]
		public void CanResolveConflictBetweenPutAndDelete_IncomingDelete_Resolver_disabled()
		{
			CanResolveConflict(resolver => resolver.Enabled = false, delegate(IDocumentStore store1, IDocumentStore store2)
			{
				using (IDocumentSession s = store2.OpenSession())
				{
					var user = s.Load<User>(docId);
					user.Active = true;
					s.Store(user);
					s.SaveChanges();
				}

				using (IDocumentSession s = store1.OpenSession())
				{
					var user = s.Load<User>(docId);
					s.Delete(user);
					s.SaveChanges();
				}

				Assert.Throws<ConflictException>(
					() => WaitForReplication(store2, session => session.Load<User>(docId) == null));
			}
				);
		}

		[Fact]
		public void CanResolveConflictBetweenPutAndDelete_IncomingPut_Resolver_disabled()
		{
			CanResolveConflict(resolver => resolver.Enabled = false, delegate(IDocumentStore store1, IDocumentStore store2)
			{
				using (IDocumentSession s = store2.OpenSession())
				{
					var user = s.Load<User>(docId);
					s.Delete(user);
					s.SaveChanges();
				}
				using (IDocumentSession s = store1.OpenSession())
				{
					var user = s.Load<User>(docId);
					user.Active = true;
					s.Store(user);
					s.SaveChanges();
				}

				Assert.Throws<ConflictException>(
					() => WaitForReplication(store2, session => session.Load<User>(docId) != null));
			}
				);
		}

		[InheritedExport(typeof (AbstractDocumentReplicationConflictResolver))]
		public class DeleteOnConflict : AbstractDocumentReplicationConflictResolver
		{
			public static DeleteOnConflict Instance;

			public DeleteOnConflict()
			{
				Instance = this;
			}

			public bool Enabled { get; set; }


			public override bool TryResolve(string id, RavenJObject metadata, RavenJObject document, JsonDocument existingDoc,
				 Func<string, JsonDocument> getDocument, 
				 out RavenJObject metadataToSave,
				 out RavenJObject documentToSave)
			{
				metadataToSave = metadata;
				documentToSave = document;
				if (Enabled)
				{
					metadata.Add("Raven-Remove-Document-Marker", true);
				}
				return Enabled;
			}
		}
	}
}