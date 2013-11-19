// -----------------------------------------------------------------------
//  <copyright file="TransformWithConversionListener.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class TransformWithConversionListener : RavenTest
    {
         public class Item
         {
			 public string Id { get; set; }

             public string Name { get; set; }
         }

        public class TransformedItem
        {
            public bool Transformed { get; set; }
            public string Name { get; set; }
            public bool Converted { get; set; }
        }

		public class EtagTransformer : AbstractTransformerCreationTask<Item>
        {
            public EtagTransformer()
            {
                TransformResults = items =>
                                   from item in items
                                   select new
                                   {
                                       item.Name,
                                       Transformed = true
                                   };
            }
        }

        public class DocumentConversionListener  : IDocumentConversionListener
        {
            public void DocumentToEntity(string key, object entityInstance, RavenJObject document, RavenJObject metadata)
            {
                ((TransformedItem) entityInstance).Converted = true;
            }

            public void EntityToDocument(string key, object entityInstance, RavenJObject document, RavenJObject metadata)
            {
            }
        }

        [Fact]
        public void SyncApi()
        {
            using (var store = (DocumentStore)NewRemoteDocumentStore())
            {
				new EtagTransformer().Execute(store);

                store.RegisterListener(new DocumentConversionListener());

                using (var session = store.OpenSession())
                {
                    session.Store(new Item{Name = "oren"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
					var transformedItem = session.Load<EtagTransformer, TransformedItem>("items/1");
                    Assert.True(transformedItem.Transformed);
                    Assert.True(transformedItem.Converted);
                }
            }
        }


        [Fact]
        public void AsyncApi()
        {
            using (var store = (DocumentStore)NewRemoteDocumentStore())
            {
				new EtagTransformer().ExecuteAsync(store.AsyncDatabaseCommands, store.Conventions).Wait();

                store.RegisterListener(new DocumentConversionListener());

                using (var session = store.OpenAsyncSession())
                {
                    session.StoreAsync(new Item { Name = "oren" }).Wait();
                    session.SaveChangesAsync().Wait();
                }

                using (var session = store.OpenAsyncSession())
                {
					var transformedItem = session.LoadAsync<EtagTransformer, TransformedItem>("items/1").Result;
                    Assert.True(transformedItem.Transformed);
                    Assert.True(transformedItem.Converted);
                }
            }
        }
    }
}