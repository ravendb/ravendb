using Raven.Abstractions.Connection;
using Raven.Abstractions.Json;
using Raven.Client;
using Raven.Abstractions.Connection;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.MailingList;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Raven.Client.Connection;

namespace Raven.Tests.Issues
{
    public class RavenDB_4103:ReplicationBase
    {
        [Fact]
        public void DeleteConflitDocumentsFirstMainAfter()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            SetupReplication(store1.DatabaseCommands, new RavenJObject[] {
             new RavenJObject { { "Url", store2.Url }, { "Disabled", true } }});
            
            using (var session = store1.OpenSession())
            {
                session.Store(new Person
                {
                    Name = "Foo"
                },"People/1");
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new Person
                {
                    Name = "Bar"
                }, "People/1");
                session.SaveChanges();
            }
            
            ToggleReplication(store1, disabled: false);            
            
            RavenJObject conflictPerson = null;
            WaitForReplication(store2, session => GetConflictDocument(store2, "People/1", out conflictPerson));
            using (var session = store2.OpenSession())
            {                

                var conflictsArray = conflictPerson.Value<RavenJArray>("Conflicts");
                
                Assert.NotEmpty(conflictsArray);
                
                foreach(var conflict in conflictsArray)
                {
                    store2.DatabaseCommands.Delete(conflict.ToString(), null);
                }                    
                
                store2.DatabaseCommands.Delete("People/1", null);                
            }
        }

        private bool GetConflictDocument(IDocumentStore store, string id, out RavenJObject conflictsJObject)
        {
            conflictsJObject = null;
            try
            {
                var serverClient = (ServerClient)store.DatabaseCommands;
                store.JsonRequestFactory.CreateHttpJsonRequest(new
                        CreateHttpJsonRequestParams(store.DatabaseCommands,
                        store.Url + "/docs/" + id, "GET", serverClient.Credentials, store.Conventions)).ExecuteRequest();
                return false;
            }
            catch (WebException e)
            {
                var httpWebResponse = e.Response as HttpWebResponse;
                if (httpWebResponse == null)
                    throw;
                if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
                {
                    var conflicts = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression());
                    conflictsJObject = RavenJObject.Load(new RavenJsonTextReader(conflicts));
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception)
            {
                return false;
            }            
        }

        private static void ToggleReplication(Client.IDocumentStore store1, bool disabled)
        {
            using (var session = store1.OpenSession())
            {
                var docAsRJO = store1.DatabaseCommands.Get(@"Raven/Replication/Destinations");
                var repDoc1 = session.Load<Raven.Abstractions.Replication.ReplicationDocument>(@"Raven/Replication/Destinations");
                foreach (var dest in repDoc1.Destinations)
                {
                    dest.Disabled = disabled;
                }

                session.SaveChanges();
            }
        }
    }
}
