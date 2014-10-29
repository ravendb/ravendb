//-----------------------------------------------------------------------
// <copyright file="OperationHeaders.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class OperationHeaders : RavenTest
	{
		[Fact]
		public void CanPassOperationHeadersUsingEmbedded()
		{
			using (var documentStore = NewDocumentStore(configureStore: store => store.Configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof (RecordOperationHeaders)))))
			{
				RecordOperationHeaders.Hello = null;
				using(var session = documentStore.OpenSession())
				{
					((DocumentSession)session).DatabaseCommands.OperationsHeaders["Hello"] = "World";
					session.Store(new { Bar = "foo"});
					session.SaveChanges();

					Assert.Equal("World", RecordOperationHeaders.Hello);
				}
			}
		}

		public class RecordOperationHeaders : AbstractPutTrigger
		{
			public static string Hello;

			public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
			{
				Hello = CurrentOperationContext.Headers.Value["Hello"];
				base.OnPut(key, document, metadata, transactionInformation);
			}
		}
	}
}