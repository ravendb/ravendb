//-----------------------------------------------------------------------
// <copyright file="CascadeDelete.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.ComponentModel.Composition.Hosting;
using System.IO;

using Raven.Abstractions.Data;
using Raven.Bundles.CascadeDelete;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.CascadeDelete
{
    public class CascadeDelete : RavenTest
    {
        private readonly DocumentStore documentStore;

        public CascadeDelete()
        {
            var ravenDbServer = GetNewServer(activeBundles: "Cascade Delete", databaseName: Constants.SystemDatabase, configureConfig: configuration =>
            {
                configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof (CascadeDeleteTrigger).Assembly));
            });

            documentStore = NewRemoteDocumentStore(ravenDbServer: ravenDbServer, databaseName: Constants.SystemDatabase);
        }

        [Fact]
        public void Can_delete_document_with_no_cascades_specified()
        {
            var master = new CascadeTester { Name = "Master" };

            using (var session = documentStore.OpenSession())
            {
                session.Store(master);
                session.SaveChanges();
            }

            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master created
                Assert.NotNull(session.Load<CascadeTester>(master.Id));
            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master.Id));
                session.SaveChanges();
                // assert master deleted
                Assert.Null(session.Load<CascadeTester>(master.Id));
            }

        }


        [Fact]
        public void Can_delete_document_with_not_existing_cascades_specified()
        {
            var master = new CascadeTester { Name = "Master" };

            using (var session = documentStore.OpenSession())
            {
                session.Store(master);
                session.Advanced.GetMetadataFor(master)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray {"I_Dont_Exist"};
                session.Advanced.GetMetadataFor(master)[MetadataKeys.AttachmentsToCascadeDelete] = new RavenJArray {"Neither_Do_I"};
                session.SaveChanges();
            }

            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master created
                Assert.NotNull(session.Load<CascadeTester>(master.Id));
            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master.Id));
                session.SaveChanges();
                // assert master deleted
                Assert.Null(session.Load<CascadeTester>(master.Id));
            }

        }


        [Fact]
        public void Can_cascade_delete_single_referenced_document()
        {
            var master = new CascadeTester {Name="Master"};
            var child = new CascadeTester {Name="Child"};
            

            using (var session = documentStore.OpenSession())
            {
                session.Store(master);
                session.Store(child);
                session.Advanced.GetMetadataFor(master)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray {child.Id};
                session.SaveChanges();
            }

            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master created
                Assert.NotNull(session.Load<CascadeTester>(master.Id));
                // assert child created
                Assert.NotNull(session.Load<CascadeTester>(child.Id));
            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master.Id));
                session.SaveChanges();
                // assert master deleted
                Assert.Null(session.Load<CascadeTester>(master.Id));
                // assert child deleted
                Assert.Null(session.Load<CascadeTester>(child.Id));
            }

        }

        [Fact]
        public void Can_cascade_delete_multiple_referenced_documents()
        {
            var master = new CascadeTester {Name="Master"};
            var child1 = new CascadeTester {Name="Child 1"};
            var child2 = new CascadeTester {Name="Child 2"};
            
            using (var session = documentStore.OpenSession())
            {
                session.Store(master);
                session.Store(child1);
                session.Store(child2);
                session.Advanced.GetMetadataFor(master)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray(child1.Id, child2.Id);
                session.SaveChanges();
            }

            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master created
                Assert.NotNull(session.Load<CascadeTester>(master.Id));
                // assert child 1 created
                Assert.NotNull(session.Load<CascadeTester>(child1.Id));
                // assert child 2 created
                Assert.NotNull(session.Load<CascadeTester>(child2.Id));
            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master.Id));
                session.SaveChanges();
                // assert master deleted
                Assert.Null(session.Load<CascadeTester>(master.Id));
                // assert child 1 deleted
                Assert.Null(session.Load<CascadeTester>(child1.Id));
                // assert child 2 deleted
                Assert.Null(session.Load<CascadeTester>(child1.Id));
            }

        }


        [Fact]
        public void Can_cascade_delete_single_referenced_attachment()
        {
            var master = new CascadeTester {Name="Master"};
            
            using (var session = documentStore.OpenSession())
            {
                session.Store(master);
                documentStore.DatabaseCommands.PutAttachment("Cascade-Delete-Me", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                session.Advanced.GetMetadataFor(master)[MetadataKeys.AttachmentsToCascadeDelete] = new RavenJArray{"Cascade-Delete-Me"};
                session.SaveChanges();
            }



            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master created
                Assert.NotNull(session.Load<CascadeTester>(master.Id));
                // assert attachment created
                Assert.NotNull(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me"));
            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master.Id));
                session.SaveChanges();
                // assert master deleted
                Assert.Null(session.Load<CascadeTester>(master.Id));
                // assert attachment deleted
                Assert.Null(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me"));
            }

        }

        [Fact]
        public void Can_cascade_delete_multiple_referenced_attachments()
        {
            var master = new CascadeTester {Name="Master"};
            
            using (var session = documentStore.OpenSession())
            {
                session.Store(master);
                documentStore.DatabaseCommands.PutAttachment("Cascade-Delete-Me-1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                documentStore.DatabaseCommands.PutAttachment("Cascade-Delete-Me-2", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                session.Advanced.GetMetadataFor(master)[MetadataKeys.AttachmentsToCascadeDelete] = new RavenJArray("Cascade-Delete-Me-1","Cascade-Delete-Me-2");
                session.SaveChanges();
            }



            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master created
                Assert.NotNull(session.Load<CascadeTester>(master.Id));
                // assert attachment 1 created
                Assert.NotNull(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-1"));
                // assert attachment 2 created
                Assert.NotNull(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-2"));

            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master.Id));
                session.SaveChanges();
                // assert master deleted
                Assert.Null(session.Load<CascadeTester>(master.Id));
                // assert attachment 1 deleted
                Assert.Null(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-1"));
                // assert attachment 2 deleted
                Assert.Null(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-2"));

            }

        }

        [Fact]
        public void Can_cascade_delete_multiple_referenced_documents_and_attachments()
        {
            var master = new CascadeTester {Name="Master"};
            var child1 = new CascadeTester {Name="Child 1"};
            var child2 = new CascadeTester {Name="Child 2"}; 

            using (var session = documentStore.OpenSession())
            {
                session.Store(master);
                session.Store(child1);
                session.Store(child2);
                session.Advanced.GetMetadataFor(master)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray(child1.Id, child2.Id);
                documentStore.DatabaseCommands.PutAttachment("Cascade-Delete-Me-1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                documentStore.DatabaseCommands.PutAttachment("Cascade-Delete-Me-2", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                session.Advanced.GetMetadataFor(master)[MetadataKeys.AttachmentsToCascadeDelete] = new RavenJArray("Cascade-Delete-Me-1", "Cascade-Delete-Me-2");
                session.SaveChanges();
            }



            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master created
                Assert.NotNull(session.Load<CascadeTester>(master.Id));
                // assert child 1 created
                Assert.NotNull(session.Load<CascadeTester>(child1.Id));
                // assert child 2 created
                Assert.NotNull(session.Load<CascadeTester>(child2.Id)); 
                // assert attachment 1 created
                Assert.NotNull(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-1"));
                // assert attachment 2 created
                Assert.NotNull(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-2"));

            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master.Id));
                session.SaveChanges();
                // assert master deleted
                Assert.Null(session.Load<CascadeTester>(master.Id));
                // assert child 1 deleted
                Assert.Null(session.Load<CascadeTester>(child1.Id));
                // assert child 2 deleted
                Assert.Null(session.Load<CascadeTester>(child2.Id)); 
                // assert attachment 1 deleted
                Assert.Null(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-1"));
                // assert attachment 2 deleted
                Assert.Null(documentStore.DatabaseCommands.GetAttachment("Cascade-Delete-Me-2"));

            }

        }

        [Fact]
        public void Can_cascade_delete_documents_that_specify_each_other_as_cascade_deletes()
        {
            var master1 = new CascadeTester { Name = "Master 1" };
            var master2 = new CascadeTester { Name = "Master 2" };

            using (var session = documentStore.OpenSession())
            {
                session.Store(master1);
                session.Store(master2);
                session.Advanced.GetMetadataFor(master1)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray { master2.Id };
                session.Advanced.GetMetadataFor(master2)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray { master1.Id };
                session.SaveChanges();
            }

            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master 1 created
                Assert.NotNull(session.Load<CascadeTester>(master1.Id));
                // assert master 2 created
                Assert.NotNull(session.Load<CascadeTester>(master2.Id));
            }


            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master1.Id));
                session.SaveChanges();
                // assert master 1 deleted
                Assert.Null(session.Load<CascadeTester>(master1.Id));
                // assert master 2 deleted
                Assert.Null(session.Load<CascadeTester>(master2.Id));
            }
        }

        [Fact]
        public void Can_cascade_delete_documents_recursively()
        {
            var master1 = new CascadeTester { Name = "Master 1" };
            var middle1 = new CascadeTester { Name = "Middle 1" };
            var child1 = new CascadeTester { Name = "Child 1" };

            using (var session = documentStore.OpenSession())
            {
                session.Store(master1);
                session.Store(middle1);
                session.Store(child1);
                session.Advanced.GetMetadataFor(master1)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray { middle1.Id };
                session.Advanced.GetMetadataFor(middle1)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray { child1.Id };
                session.SaveChanges();
            }

            // assert initial creation
            using (var session = documentStore.OpenSession())
            {
                // assert master 1 created
                Assert.NotNull(session.Load<CascadeTester>(master1.Id));
                // assert middle 1 created
                Assert.NotNull(session.Load<CascadeTester>(middle1.Id));
                // assert child 1 created
                Assert.NotNull(session.Load<CascadeTester>(child1.Id));
            }

            using (var session = documentStore.OpenSession())
            {
                session.Delete<CascadeTester>(session.Load<CascadeTester>(master1.Id));
                session.SaveChanges();
                // assert master 1 deleted
                Assert.Null(session.Load<CascadeTester>(master1.Id));
                // assert middle 1 deleted
                Assert.Null(session.Load<CascadeTester>(middle1.Id));
                // assert child 1 deleted
                Assert.Null(session.Load<CascadeTester>(child1.Id));
            }
        }

        public class CascadeTester
        {
            public string Name { get; set; }

            public string Id { get; set; }
        }
    }
}
