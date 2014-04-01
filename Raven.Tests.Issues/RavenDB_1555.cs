// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1555.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Tasks;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1555 : ReplicationBase
	{
		public class Foo
		{
			public int Bar { get; set; }
		}

		[Fact]
		public void SecondMasterShouldUpdateRemoteEtagOnFirstMasterWhenNumberOfFilteredDocumentsExceedsTheLimit()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			// master - master
			SetupReplication(store1.DatabaseCommands, store2);
			SetupReplication(store2.DatabaseCommands, store1);

			using (var session = store1.OpenSession())
			{
				for (int i = 0; i < ReplicationTask.DestinationDocsLimitForRemoteEtagUpdate - 1; i++)
				{
					session.Store(new Foo
					{
						Bar = i
					});
				}

				session.SaveChanges();
			}

			WaitForReplication(store2, "foos/1");

			Assert.Equal(Etag.Empty, GetReplicatedEtag(store1, store2)); // store2 didn't update yet the last replicated etag on server1

			// put fake system document to trigger replication task execution
			store2.DatabaseCommands.Put("Raven/FakeSystemDocument", null, new RavenJObject(), new RavenJObject());

			using (var session = store1.OpenSession())
			{
				// additional document to exceed ReplicationTask.DestinationDocsLimitForRemoteEtagUpdate limit
				var entity = new Foo
				{
					Bar = 999
				};
				session.Store(entity);
				
				session.SaveChanges();

				WaitForReplication(store2, session.Advanced.GetDocumentId(entity));
			}

			// put fake system document to trigger replication task execution
			store2.DatabaseCommands.Put("Raven/FakeSystemDocument", null, new RavenJObject(), new RavenJObject());

			Etag replicatedEtag = null;

			for (int i = 0; i < 1000; i++)
			{
				replicatedEtag = GetReplicatedEtag(store1, store2);

				if(replicatedEtag != Etag.Empty)
					break;
				Thread.Sleep(100);
			}

			Assert.NotNull(replicatedEtag);
			Assert.NotEqual(Etag.Empty, replicatedEtag);
		}

		private static Etag GetReplicatedEtag(DocumentStore source, DocumentStore dest)
		{
		    var sourceUrl = source.Url + "/databases/" + source.DefaultDatabase;
		    var destinationUrl = dest.Url + "/databases/" + dest.DefaultDatabase;

			var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(
				null,
                sourceUrl.LastReplicatedEtagFor(destinationUrl),
				"GET",
				new OperationCredentials(source.ApiKey, source.Credentials),
				source.Conventions);
			var httpJsonRequest = source.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);

			var json = httpJsonRequest.ReadResponseJson();

			return Etag.Parse(json.Value<string>("LastDocumentEtag"));
		}
	}
}