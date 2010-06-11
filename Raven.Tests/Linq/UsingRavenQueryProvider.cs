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
        public string Info { get; set; }
        public bool Active { get; set; }

        public User()
        {
            Name = String.Empty;
            Age = default(int);
            Info = String.Empty;
            Active = false;
        }        
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
                db.Initialize();

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

        [Fact]
        public void Can_perform_First_and_FirstOrDefault_Query()
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
                db.Initialize();

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

                    var firstItem = session.Query<User>(indexName)
                                            .First();
                    Assert.Equal(firstUser, firstItem);

                    //This should pull out the 1st parson ages 60, i.e. "Bob"
                    var firstAgeItem = session.Query<User>(indexName)
                                            .First(x => x.Age == 60);
                    Assert.Equal("Bob", firstAgeItem.Name);

                    //No-one is aged 15, so we should get a default item back, i.e. Name = "" and Age = 0
                    var firstDefaultItem = session.Query<User>(indexName)
                                            .FirstOrDefault(x => x.Age == 15);
                    Assert.Null(firstDefaultItem);
                }
            }
        }

        [Fact]
        public void Can_perform_Single_and_SingleOrDefault_Query()
        {
            //When running in the XUnit GUI strange things happen is we just create a path relative to 
            //the .exe itself, so make our folder in the System temp folder instead ("<user>\AppData\Local\Temp")
            string directoryName = Path.Combine(Path.GetTempPath(), "ravendb.RavenQueryProvider");
            if (Directory.Exists(directoryName))
            {
                Directory.Delete(directoryName, true);
            }

            using (var db = new DocumentStore() { DataDirectory = directoryName })
            {
                db.Initialize();

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

                    var singleItem = session.Query<User>(indexName)
                                            .Single(x => x.Name == "James");
                    Assert.Equal(25, singleItem.Age);
                    Assert.Equal("James", singleItem.Name);   

                    //A default query should return for results, so Single() should throw
                    Assert.Throws(typeof(InvalidOperationException), () => session.Query<User>(indexName).Single());
                    //A query of age = 30 should return for 2 results, so Single() should throw
                    Assert.Throws(typeof(InvalidOperationException), () => session.Query<User>(indexName).Single(x => x.Age == 30));

                    //A query of age = 30 should return for 2 results, so SingleOrDefault() should also throw
                    Assert.Throws(typeof(InvalidOperationException), () => session.Query<User>(indexName).SingleOrDefault(x => x.Age == 30));

                    //A query of age = 75 should return for NO results, so SingleOrDefault() should return a default value
                    var singleOrDefaultItem = session.Query<User>(indexName)
                                            .SingleOrDefault(x => x.Age == 75);
                    Assert.Null(singleOrDefaultItem);
                }
            }
        }        

        [Fact]
        public void Can_perform_Boolean_Queries()
        {
            //When running in the XUnit GUI strange things happen is we just create a path relative to 
            //the .exe itself, so make our folder in the System temp folder instead ("<user>\AppData\Local\Temp")
            string directoryName = Path.Combine(Path.GetTempPath(), "ravendb.RavenQueryProvider");
            if (Directory.Exists(directoryName))
            {
                Directory.Delete(directoryName, true);
            }

            using (var db = new DocumentStore() { DataDirectory = directoryName })
            {
                db.Initialize();

                string indexName = "UserIndex";
                using (var session = db.OpenSession())
                {
                    session.Store(new User() { Name = "Matt", Info = "Male Age 25" }); //Active = false by default
                    session.Store(new User() { Name = "Matt", Info = "Male Age 28", Active = true });
                    session.Store(new User() { Name = "Matt", Info = "Male Age 35", Active = false });
                    session.SaveChanges();

                    db.DatabaseCommands.DeleteIndex(indexName);
                    var result = db.DatabaseCommands.PutIndex<User, User>(indexName,
                            new IndexDefinition<User, User>()
                            {
                                Map = docs => from doc in docs
                                              select new { doc.Name, doc.Age, doc.Info, doc.Active },
                            }, true);

                    WaitForQueryToComplete(session, indexName);

                    var testQuery = session.Query<User>(indexName)
                                        .Where(x => x.Name == "Matt" && x.Active);                    
                    Assert.Equal(1, testQuery.ToArray().Count());
                    foreach (var testResult in testQuery)
                        Assert.True(testResult.Active);
                    
                    testQuery = session.Query<User>(indexName)
                                        .Where(x => x.Name == "Matt" && !x.Active);
                    Assert.Equal(2, testQuery.ToArray().Count());
                    foreach (var testResult in testQuery)
                        Assert.False(testResult.Active);                                       
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
        User lastUser = new User { Name = "Alan", Age = 30 };

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
