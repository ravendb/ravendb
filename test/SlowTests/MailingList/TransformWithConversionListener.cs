// -----------------------------------------------------------------------
//  <copyright file="TransformWithConversionListener.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class TransformWithConversionListener : RavenTestBase
    {
         private class Item
         {
             public string Id { get; set; }

             public string Name { get; set; }
         }

        private class TransformedItem
        {
            public bool Transformed { get; set; }
            public string Name { get; set; }
            public bool Converted { get; set; }
        }

        private class EtagTransformer : AbstractTransformerCreationTask<Item>
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

        private class DocumentConversionListener  : IDocumentConversionListener
        {
            
            public void BeforeConversionToDocument(string key, object entity, RavenJObject metadata)
            {

            }

            public void AfterConversionToDocument(string key, object entity, RavenJObject document, RavenJObject metadata)
            {
            }

            public void BeforeConversionToEntity(string key, RavenJObject document, RavenJObject metadata)
            {
            }

            public void AfterConversionToEntity(string key, RavenJObject document, RavenJObject metadata, object entity)
            {
                ((TransformedItem)entity).Converted = true;
            }
        }

        [Fact]
        public void SyncApi()
        {
            using (var store = GetDocumentStore())
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
            using (var store = GetDocumentStore())
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
