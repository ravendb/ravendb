// -----------------------------------------------------------------------
//  <copyright file="RavenReplicationCoreTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Core.Replication
{
	public class RavenReplicationCoreTest : RavenCoreTestBase
	{
		protected int RetriesCount = 500;

		protected override DocumentStore GetDocumentStore([CallerMemberName] string databaseName = null, 
															string dbSuffixIdentifier = null,
															Action<DatabaseDocument> modifyDatabaseDocument = null)
		{
			return base.GetDocumentStore(databaseName, dbSuffixIdentifier ?? createdStores.Count.ToString(CultureInfo.InvariantCulture), 
											modifyDatabaseDocument: doc => doc.Settings.Add("Raven/ActiveBundles", "replication"));
		}

		protected void SetupReplication(DocumentStore source, params DocumentStore[] destinations)
		{
			Assert.NotEmpty(destinations);
			SetupReplication(source, destinations.Select(destination => new RavenJObject
                                                                        {
                                                                            { "Url", destination.Url },
                                                                            { "Database", destination.DefaultDatabase }
                                                                        }));
		}

		protected void SetupReplication(DocumentStore source, IEnumerable<RavenJObject> destinations)
		{
			source.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
					   null, new RavenJObject
                       {
                           {
                               "Destinations", new RavenJArray(destinations)
                           }
                       }, new RavenJObject());
		}

		protected Attachment WaitForAttachment(DocumentStore destination, string attachmentName)
		{
			Attachment attachment = null;

			for (int i = 0; i < RetriesCount; i++)
			{
				attachment = destination.DatabaseCommands.GetAttachment(attachmentName);
				if (attachment != null)
					break;
				Thread.Sleep(100);
			}
			try
			{
				Assert.NotNull(attachment);
			}
			catch (Exception ex)
			{
				Thread.Sleep(TimeSpan.FromSeconds(10));

				attachment = destination.DatabaseCommands.GetAttachment(attachmentName);
				if (attachment == null)
					throw;

				throw new Exception(
					"WaitForAttachment failed, but after waiting 10 seconds more, WaitForAttachment succeed. Do we have a race condition here?",
					ex);
			}
			return attachment;
		}

		protected TDocument WaitForDocument<TDocument>(DocumentStore store2, string expectedId) where TDocument : class
		{
			TDocument document = null;

			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenSession())
				{
					document = session.Load<TDocument>(expectedId);
					if (document != null)
						break;
					Thread.Sleep(100);
				}
			}
			try
			{
				Assert.NotNull(document);
			}
			catch (Exception ex)
			{
				using (var session = store2.OpenSession())
				{
					Thread.Sleep(TimeSpan.FromSeconds(10));

					document = session.Load<TDocument>(expectedId);
					if (document == null)
						throw;

					throw new Exception("WaitForDocument failed, but after waiting 10 seconds more, WaitForDocument succeed. Do we have a race condition here?", ex);
				}
			}
			return document;
		}

		protected JsonDocument WaitForDocument(DocumentStore store, string expectedId)
		{
			JsonDocument result = null;

			for (int i = 0; i < RetriesCount; i++)
			{
				result = store.DatabaseCommands.Get(expectedId);
				if (result != null)
					break;
				Thread.Sleep(100);
			}

			Assert.NotNull(result);

			return result;
		}
	}
}