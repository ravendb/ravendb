using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
 
 namespace Raven.Tests.Issues
 {
     public class RavenDB_4750 : RavenTestBase
     {
         public class FooBar
         {
             public string Text { get; set; }	
         }
 
         public class BarFoo
         {			
         }
 
         [Fact(Timeout = 60000 * 30)] //do not run forever...
         public void Three_node_cluster_should_not_go_to_infinite_loop()
         {
             //const int BatchSize = 1000;
 
             using (var r1Server = GetNewServer(8067, configureConfig: configuration =>
              {
 configuration.Settings["Raven/ActiveBundles"] = "Replication";
             }))
             using (var r1 = NewRemoteDocumentStore(ravenDbServer: r1Server))
             using (var r2Server = GetNewServer(8068, configureConfig: configuration =>
             {
 configuration.Settings["Raven/ActiveBundles"] = "Replication";
             }))
             using (var r2 = NewRemoteDocumentStore(ravenDbServer: r2Server))
             using (var r3Server = GetNewServer(8069, configureConfig: configuration =>
             {
 configuration.Settings["Raven/ActiveBundles"] = "Replication";
             }))
             using (var r3 = NewRemoteDocumentStore(ravenDbServer: r3Server))
             {
                 CreateDatabaseWithReplication(r1, "testDB");
                 CreateDatabaseWithReplication(r2, "testDB");
                 CreateDatabaseWithReplication(r3, "testDB");
 
                 SetupReplication(r1, "testDB", r2, r3);
                 SetupReplication(r2, "testDB", r1, r3);
                 SetupReplication(r3, "testDB", r1, r2);
 
                 const int count = 50000;
                 using (var bulkInsert = r1.BulkInsert("testDB"))
                     for (int i = 0; i<count; i++)
                         bulkInsert.Store(new FooBar {Text = "foobar" + i}, "foobar/" + i);
 
                 WaitForDocumentCount(r2, "testDB", count);
                 WaitForDocumentCount(r3, "testDB", count);
 
                 using (var session = r2.OpenSession("testDB"))
                 {
                     session.Store(new BarFoo(),"test/1");
                     session.SaveChanges();
                 }

                 Assert.True(WaitForReplicationOfDocument<BarFoo>(r2, "testDB", "test/1"));
                 Assert.True(WaitForReplicationOfDocument<BarFoo>(r3, "testDB", "test/1"));
                 Assert.True(WaitForReplicationOfDocument<BarFoo>(r1, "testDB", "test/1"));
             }
         }
 
         private void WaitForDocumentCount(IDocumentStore store, string db, int count)
         {
             long currentCount = 0;
             while (currentCount<count)
             {
                 currentCount = store.DatabaseCommands.ForDatabase(db).GetStatistics().CountOfDocuments;
                 Thread.Sleep(500);
             }
         }
 
         //do not run forever
         private bool WaitForReplicationOfDocument<T>(IDocumentStore store, string db, string docId, int timeout = 60000 * 5)
         {
             var sw = Stopwatch.StartNew();
             while(sw.ElapsedMilliseconds<timeout)
             {
                 using (var session = store.OpenSession(db))
                 {
                     var doc = session.Load<T>(docId);
                     if (doc != null)
                         return true;
                     Thread.Sleep(100);
                 }
             }
 
             return false;
         }
 
 
         private static void SetupReplication(IDocumentStore source, string databaseName, params IDocumentStore[] destinations)
         {
             source
                 .DatabaseCommands
                 .ForDatabase(databaseName)
                 .Put(
                     Constants.RavenReplicationDestinations,
                     null,
                     RavenJObject.FromObject(new ReplicationDocument
                     {
                         Destinations = new List<ReplicationDestination>(destinations.Select(destination =>
                             new ReplicationDestination
                             {
                                 Database = databaseName,
                                 Url = destination.Url,
                                 TransitiveReplicationBehavior = TransitiveReplicationOptions.None					
                             }))
                     }),
                     new RavenJObject());
         }	
 
         private static void CreateDatabaseWithReplication(DocumentStore store, string databaseName)
         {
             store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
             {
                 Id = databaseName,
                 Settings =
                 {
                     {"Raven/DataDir", "~/Tenants/" + databaseName},
                     {"Raven/ActiveBundles", "Replication"}
                 }
             });
         }
     }
 }