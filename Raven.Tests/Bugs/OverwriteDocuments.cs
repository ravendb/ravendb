//-----------------------------------------------------------------------
// <copyright file="OverwriteDocuments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Http.Exceptions;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class OverwriteDocuments : IDisposable
    {
        private DocumentStore documentStore;

        private void CreateFreshDocumentStore() {
            if (documentStore != null)
                documentStore.Dispose();

            IOExtensions.DeleteDirectory("HiLoData");
            documentStore = new EmbeddableDocumentStore
            {
            	Configuration =
            		{
            			DataDirectory = "HiLoData"
            		}
            };
        	documentStore.Initialize();

            documentStore.DatabaseCommands.PutIndex("Foo/Something", new IndexDefinition<Foo> {
                                                                                                  Map = docs => from doc in docs select new { doc.Something }
                                                                                              });
        }

        [Fact]
        public void Will_throw_if_asked_to_store_new_document_which_exists_when_optimistic_concurrency_is_on()
        {
            CreateFreshDocumentStore();

            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Id = "foos/1", Something = "something1" };
                session.Store(foo);
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
            	session.Advanced.UseOptimisticConcurrency = true;
				var foo = new Foo() { Id = "foos/1", Something = "something1" };
                session.Store(foo);
            	Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
            }
        }

		[Fact]
		public void Will_overwrite_doc_if_asked_to_store_new_document_which_exists_when_optimistic_concurrency_is_off()
		{
			CreateFreshDocumentStore();

			using (var session = documentStore.OpenSession())
			{
				var foo = new Foo() { Id = "foos/1", Something = "something1" };
				session.Store(foo);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var foo = new Foo() { Id = "foos/1", Something = "something1" };
				session.Store(foo);
				session.SaveChanges();
			}
		}


        public class Foo
        {
            public string Id { get; set; }
            public string Something { get; set; }
        }

        public void Dispose()
        {
            if (documentStore != null)
                documentStore.Dispose();
            Thread.Sleep(100);
            IOExtensions.DeleteDirectory("HiLoData");
        }
    }
}
