using System;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Responders;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bundles.Replication.Issues
{
	public abstract class RavenDB677 : ReplicationBase
	{
		 [Fact]
		 public void CanDeleteTombstones()
		 {
			 var store1 = (DocumentStore)CreateStore();
			var x= store1.DatabaseCommands.Put("ayende", null, new RavenJObject(), new RavenJObject());
			 store1.DatabaseCommands.Delete("ayende", null);
			 servers[0].Database.TransactionalStorage.Batch(accessor =>
			 {
				 Assert.NotEmpty(accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Guid.Empty, 10));
			 });

			 var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
			                                                                   servers[0].Database.ServerUrl +
																			   "admin/replication/purge-tombstones?etag=00000000-0000-0100-0000-000000000003",
			                                                                   "POST",
			                                                                   CredentialCache.DefaultCredentials,
			                                                                   store1.Conventions);
			 store1.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ExecuteRequest();


			 servers[0].Database.TransactionalStorage.Batch(accessor =>
			 {
				 Assert.Empty(accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Guid.Empty, 10));
			 });
		 }

		 [Fact]
		 public void CanDeleteTombstonesButNotAfterTheSpecifiedEtag()
		 {
			 var store1 = (DocumentStore)CreateStore();
			 store1.DatabaseCommands.Put("ayende", null, new RavenJObject(), new RavenJObject());
			 store1.DatabaseCommands.Delete("ayende", null);
			 store1.DatabaseCommands.Put("rahien", null, new RavenJObject(), new RavenJObject());
			 store1.DatabaseCommands.Delete("rahien", null);
			 servers[0].Database.TransactionalStorage.Batch(accessor =>
			 {
				 Assert.Equal(2, accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Guid.Empty, 10).Count());
			 });

			 var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
																			   servers[0].Database.ServerUrl +
																			   "admin/replication/purge-tombstones?etag=00000000-0000-0100-0000-000000000003",
																			   "POST",
																			   CredentialCache.DefaultCredentials,
																			   store1.Conventions);
			 store1.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ExecuteRequest();


			 servers[0].Database.TransactionalStorage.Batch(accessor =>
			 {
				 Assert.Equal(1, accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, Guid.Empty, 10).Count());
			 });
		 }
	}

	public class RavenDb677_Munin : RavenDB677
	{
		protected override void ConfigureServer(Database.Config.RavenConfiguration serverConfiguration)
		{
			serverConfiguration.DefaultStorageTypeName = "munin";
		}
	}

	public class RavenDb677_Esent : RavenDB677
	{
		protected override void ConfigureServer(Database.Config.RavenConfiguration serverConfiguration)
		{
			serverConfiguration.DefaultStorageTypeName = "esent";
			serverConfiguration.RunInMemory = false;
		}
	}
}