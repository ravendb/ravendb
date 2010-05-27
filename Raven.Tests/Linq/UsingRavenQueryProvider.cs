using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Raven.Database.Data;
using Raven.Client;
using System.IO;
using Raven.Client.Document;
using Raven.Client.Linq;
using System.Threading;
using System.Diagnostics;

namespace Raven.Tests.Linq
{
    public class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
    public class UsingRavenQueryProvider
    {
        [Fact]
        public void Can_perform_Skip_Take_Query()
        {

            //When running in the XUnit GUI strange things happen is we just create a path relative to 
            //the .exe itself, so make our folder in the System temp folder instead ("<user>\AppData\Local\Temp")
            string directoryName =  Path.Combine(Path.GetTempPath(), "ravendb.RavenQueryProvider");
            if (Directory.Exists(directoryName))
            {
                Directory.Delete(directoryName, true);
            }           

            using (var db = new DocumentStore() { DataDirectory = directoryName })
            {
                db.Initialise();

                string indexName = "UserIndex";
                using (var session = db.OpenSession())
	            {
                    AddData(session);                    

                    db.DatabaseCommands.DeleteIndex(indexName);
                    var result = db.DatabaseCommands.PutIndex<User, User>(indexName,
                            new IndexDefinition<User, User>()
                            {
                                Map = docs => from doc in docs
                                              select new { doc.Name, doc.Age },
                            }, true);                    

                    WaitForQueryToComplete(session, indexName);

                    var allResults = session.Query<User>(indexName)
                                            .Where(x => x.Age > 0);
                    Assert.Equal(4, allResults.ToArray().Count());

                    var takeResults = session.Query<User>(indexName)
                                            .Where(x => x.Age > 0)
                                            .Take(3);
                    //There are 4 items of data in the db, but using Take(1) means we should only see 4
                    Assert.Equal(3, takeResults.ToArray().Count());

                    var skipResults = session.Query<User>(indexName)
                                            .Where(x => x.Age > 0)
                                            .Skip(1);
                    //Using Skip(1) means we should only see the last 3
                    Assert.Equal(3, skipResults.ToArray().Count());
                    Assert.DoesNotContain<User>(firstUser, skipResults.ToArray());                    

                    var skipTakeResults = session.Query<User>(indexName)
                                            .Where(x => x.Age > 0)
                                            .Skip(1)
                                            .Take(2);
                    //Using Skip(1), Take(2) means we shouldn't see the 1st or 4th (last) users
                    Assert.Equal(2, skipTakeResults.ToArray().Count());
                    Assert.DoesNotContain<User>(firstUser, skipTakeResults.ToArray());
                    Assert.DoesNotContain<User>(lastUser, skipTakeResults.ToArray());                    
	            }
            }            
        }

        private static void WaitForQueryToComplete(IDocumentSession session, string indexName)
        {            
            QueryResult results;
            do
            {
                //doesn't matter what the query is here, just want to see if it's stale or not
                results = session.LuceneQuery<User>(indexName)                              
                              .Where("") 
                              .QueryResult;   

                if (results.IsStale)
                    Thread.Sleep(1000);
            } while (results.IsStale);            
        }

        User firstUser = new User { Name = "Matt", Age = 30 };
        User lastUser = new User { Name = "Matt", Age = 30 };

        private void AddData(IDocumentSession documentSession)
        {
            documentSession.Store(firstUser);
            documentSession.Store(new User { Name = "James", Age = 25 });
            documentSession.Store(new User { Name = "Bob", Age = 60 });
            documentSession.Store(lastUser);

            documentSession.SaveChanges();
        }
    }
}
