using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Bugs;
using Raven.Tests.Common;
using Raven.Tests.FileSystem;
using Raven.Tests.Raft.Client;
using Raven.Tests.Smuggler;
using Raven.Tests.Subscriptions;
using Xunit;
using Order = Raven.Tests.Common.Dto.Faceted.Order;
using Raven.Tests.Raft;
using Raven.Tests.Faceted;
using Raven.Abstractions.Replication;
using Raven.Tests.Bundles.LiveTest;
using Raven.Tests.Core.BulkInsert;
using Raven.Tests.Notifications;
using Raven.Database.Bundles.SqlReplication;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Embedded;

using Raven.Tests.Common.Util;
using Raven.Client;
#if !DNXCORE50
using Raven.Tests.Sorting;
using Raven.SlowTests.RavenThreadPool;
using Raven.Tests.Core;
using Raven.Tests.Core.Commands;
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{

    public class User
    {
        public string Name;
        public int Age;
        public List<string> Cats;
        public List<string> Dogs;
    }

    public class Cat
    {
        public string Name;
        public int Age;
    }

    public class Dog
    {
        public string Name;
        public int Age;
    }

    public class Horse
    {
        public string Name;
        public int Age;
    }

    public class Raven
    {
        public string Name;
        public int Age;
    }

    public class Peageon
    {
        public string Name;
        public int Age;
    }

    public class Rock
    {
        public string Name;
        public int Age;
    }

    public class Chair
    {
        public string Name;
        public int Age;
    }


    public class UsersByAge : AbstractIndexCreationTask<User>
    {
        public UsersByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }

    public class CatsByAge : AbstractIndexCreationTask<Cat>
    {
        public CatsByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }

    public class DogsByAge : AbstractIndexCreationTask<Dog>
    {
        public DogsByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }

    public class HorsesByAge : AbstractIndexCreationTask<Horse>
    {
        public HorsesByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }

    public class RavensByAge : AbstractIndexCreationTask<Raven>
    {
        public RavensByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }

    public class PeageonsByAge : AbstractIndexCreationTask<Peageon>
    {
        public PeageonsByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }

    public class RocksByAge : AbstractIndexCreationTask<Rock>
    {
        public RocksByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }

    public class ChairsByAge : AbstractIndexCreationTask<Chair>
    {
        public ChairsByAge()
        {
            Map = users => from user in users
                           select new
                           {
                               user.Age
                           };
            Sort(x => x.Age, Abstractions.Indexing.SortOptions.Int);
        }
    }


    public class UsresAndCatsIndex : AbstractIndexCreationTask<User, UsresAndCatsIndex.Result>
    {
        public class Result
        {
            public string CatName;
            public double CatAge;
            public string UserName;
        }

        public UsresAndCatsIndex()
        {
            Map = users => from user in users
                           from cat in user.Cats
                           let catDoc = LoadDocument<Cat>(cat) ?? new Cat
                           {
                               Name = "foo",
                               Age = 9
                           }
                           select new Result
                           {
                               CatName = catDoc.Name,
                               CatAge = catDoc.Age,
                               UserName = user.Name
                           };
        }
    }

    public class DTCSqlReplicationTest
    {
        private const int DocsCount =  10000;
        private const int port = 8079;

        public void SqlReplicationTest()
        {
            using (var sysStore = new EmbeddableDocumentStore()
            {
                DataDirectory = "C:/temp/embedded2/Databases/system",
                RunInMemory = false,
                UseEmbeddedHttpServer = true,
                Configuration =
                {
                    Port = 8079
                }
            }.Initialize())
            {

                var sqlReplicationDBName = "SqlReplication";
                var deletesSourceDBName = "DeletesSource";
                ResetDBs(sysStore, sqlReplicationDBName, deletesSourceDBName);

                using (var deletesDB = new DocumentStore
                {
                    Url = "http://localhost:" + port,
                    DefaultDatabase = deletesSourceDBName
                }.Initialize())
                using (var sqlReplicationDB = new DocumentStore
                {
                    Url = "http://localhost:" + port,
                    DefaultDatabase = sqlReplicationDBName
                }.Initialize())
                {


                    Console.WriteLine("Initialized");
                    DefineReplicationFromDeletedToSqlReplication(sqlReplicationDBName, deletesSourceDBName, deletesDB, sqlReplicationDB);

                    GenerateSqlReplicationTasks(sqlReplicationDB);

                    new UsersByAge().Execute(deletesDB);
                    new CatsByAge().Execute(deletesDB);
                    new DogsByAge().Execute(deletesDB);
                    new HorsesByAge().Execute(deletesDB);
                    new RavensByAge().Execute(deletesDB);
                    new PeageonsByAge().Execute(deletesDB);
                    new UsresAndCatsIndex().Execute(deletesDB);
                    new RavenDocumentsByEntityName().Execute(deletesDB);


                    new UsersByAge().Execute(sqlReplicationDB);
                    new CatsByAge().Execute(sqlReplicationDB);
                    new DogsByAge().Execute(sqlReplicationDB);
                    new HorsesByAge().Execute(sqlReplicationDB);
                    new RavensByAge().Execute(sqlReplicationDB);
                    new PeageonsByAge().Execute(sqlReplicationDB);
                    new UsresAndCatsIndex().Execute(sqlReplicationDB);
                    new RavenDocumentsByEntityName().Execute(sqlReplicationDB);

                    List<string> UsersIDs = new List<string>();
                    List<string> CatsIDs = new List<string>();
                    List<string> DogsIDs = new List<string>();
                    List<string> HorsesIDs = new List<string>();
                    List<string> RavensIDs = new List<string>();
                    List<string> PeageonsIDs = new List<string>();
                    List<string> RocksIDs = new List<string>();
                    List<string> ChairsIDs = new List<string>();


                    using (var bi = deletesDB.BulkInsert())
                    {

                        for (var i = 0; i < DocsCount / 10; i++)
                        {
                            List<string> catsIDs = new List<string>();
                            List<string> dogsIDs = new List<string>();
                            for (var j = 0; j < 10; j++)
                            {
                                int age = (i * 10 + j) % 120;
                                var curCatID = bi.Store(new Cat
                                {
                                    Age = age,
                                    Name = "Vasia"
                                });
                                catsIDs.Add(curCatID);

                                var curDogID = bi.Store(new Dog
                                {
                                    Age = age,
                                    Name = "Shoorik"
                                });
                                catsIDs.Add(curDogID);

                                if (age <= 90)
                                {
                                    CatsIDs.Add(curCatID);
                                    DogsIDs.Add(curDogID);
                                }
                            }
                            int curUserAge = (i * 10) % 120;
                            var curUserId = bi.Store(new User
                            {
                                Name = "Petya",
                                Age = curUserAge,
                                Cats = catsIDs,
                                Dogs = dogsIDs
                            });
                            var curHorseId = bi.Store(new Horse
                            {
                                Name = "Smokie",
                                Age = curUserAge,
                            });
                            var curRavenId = bi.Store(new Raven
                            {
                                Name = "Voron",
                                Age = curUserAge,
                            });
                            var curPeogeonId = bi.Store(new Peageon
                            {
                                Name = "Yona",
                                Age = curUserAge,
                            });
                            var curRockId = bi.Store(new Rock
                            {
                                Name = "Even",
                                Age = curUserAge,
                            });
                            var curChairId = bi.Store(new Chair
                            {
                                Name = "Kisse",
                                Age = curUserAge,
                            });

                            if (curUserAge < 90)
                            {
                                UsersIDs.Add(curUserId);
                                HorsesIDs.Add(curHorseId);
                                RavensIDs.Add(curRavenId);
                                PeageonsIDs.Add(curPeogeonId);
                                RocksIDs.Add(curRockId);
                                ChairsIDs.Add(curChairId);
                            }
                        }
                    }

                    Console.WriteLine("Docs stored");

                    Console.WriteLine("DeleteDB");

                    var usersCount = 0;

                    using (var session = deletesDB.OpenSession())
                    {
                        session.Advanced.MaxNumberOfRequestsPerSession = 1000000;

                        usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var catsCount = session.Query<Cat>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var dogsCount = session.Query<Dog>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        Console.WriteLine($"{usersCount} users expected");
                        Console.WriteLine($"{catsCount} cats expected");
                        Console.WriteLine($"{dogsCount} dogs expected");
                    }

                    Console.WriteLine("SQL replication db");
                    using (var session = sqlReplicationDB.OpenSession())
                    {
                        session.Advanced.MaxNumberOfRequestsPerSession = 1000000;

                        int tempUsersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        int curTempUsersCount = 0;

                        while ((curTempUsersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count()) < usersCount)
                        {
                            if (curTempUsersCount == tempUsersCount)
                                break;
                            Thread.Sleep(1000);
                        }
                        usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var catsCount = session.Query<Cat>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var dogsCount = session.Query<Dog>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        Console.WriteLine($"{usersCount} users expected");
                        Console.WriteLine($"{catsCount} cats expected");
                        Console.WriteLine($"{dogsCount} dogs expected");
                    }


                    Console.WriteLine($"{UsersIDs.Count} registered, wait for all documents to be replicated and then press any key");
                    Console.ReadLine();

                    List<string> usersBatchToDeleteOnSource = new List<string>();
                    List<string> usersBatchToDeleteOnDest = new List<string>();
                    var skipped = 0;
                    var created = 0;

                    var rand = new System.Random();

                    var curRand = rand.Next(3, 6);


                    //while ((curUsersBatchToDelete = UsersIDs.Skip(skipped).Take(curRand).ToList()).Count > 0 )
                    while ((usersBatchToDeleteOnSource = UsersIDs.Skip(skipped).Take(curRand).ToList()).Count > 0 &&
                        (usersBatchToDeleteOnDest = UsersIDs.Skip(UsersIDs.Count / 2 + skipped).Take(curRand).ToList()).Count > 0 &&
                        skipped < UsersIDs.Count / 2)
                    {
                        using (var originSession = deletesDB.OpenSession())
                        using (var sqlReplicationSession = sqlReplicationDB.OpenSession())
                        {
                            DeletesOnSession(CatsIDs, DogsIDs, HorsesIDs, RavensIDs, PeageonsIDs, RocksIDs, usersBatchToDeleteOnSource, skipped, rand, curRand, originSession);
                            DeletesOnSession(CatsIDs, DogsIDs, HorsesIDs, RavensIDs, PeageonsIDs, RocksIDs, usersBatchToDeleteOnDest, UsersIDs.Count/2+ skipped, rand, curRand, sqlReplicationSession);
                        }
                        skipped += curRand;
                        curRand = rand.Next(3, 6);
                    }


                    Console.WriteLine("DeleteDB");
                    using (var session = deletesDB.OpenSession())
                    {
                        session.Advanced.MaxNumberOfRequestsPerSession = 1000000;

                        usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var catsCount = session.Query<Cat>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var dogsCount = session.Query<Dog>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        Console.WriteLine($"{usersCount} users expected");
                        Console.WriteLine($"{catsCount} cats expected");
                        Console.WriteLine($"{dogsCount} dogs expected");
                    }

                    Console.WriteLine("SQL replication db");
                    using (var session = sqlReplicationDB.OpenSession())
                    {
                        session.Advanced.MaxNumberOfRequestsPerSession = 1000000;

                        int tempUsersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        int curTempUsersCount= 0;
                        Console.WriteLine($"tempUsersCound: {tempUsersCount}");

                        Thread.Sleep(1000);
                        while ((curTempUsersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count()) < usersCount)
                        {
                            if (curTempUsersCount == tempUsersCount)
                                break;
                            tempUsersCount = curTempUsersCount;
                            Console.WriteLine($"tempUsersCound: {tempUsersCount}");
                            Thread.Sleep(1000);
                            
                        }
                        usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var catsCount = session.Query<Cat>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        var dogsCount = session.Query<Dog>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Count();
                        Console.WriteLine($"{usersCount} users expected");
                        Console.WriteLine($"{catsCount} cats expected");
                        Console.WriteLine($"{dogsCount} dogs expected");
                    }
                    Console.WriteLine("Now debug");
                    while(true)
                        Console.ReadLine();
                }
            }
        }

        private void DeletesOnSession(List<string> CatsIDs, List<string> DogsIDs, List<string> HorsesIDs, List<string> RavensIDs, List<string> PeageonsIDs, List<string> RocksIDs, List<string> curUsersBatchToDelete, int skipped, System.Random rand, int curRand, IDocumentSession session)
        {
            session.Advanced.MaxNumberOfRequestsPerSession = 1000000;

            foreach (var item in curUsersBatchToDelete)
            {
                var user = session.Load<User>(item);
                session.Delete(user);
                session.SaveChanges();
            }

            DeleteEntities<Cat>(CatsIDs, skipped, curRand, session);
            DeleteEntities<Dog>(DogsIDs, skipped, curRand, session);
            DeleteEntities<Horse>(HorsesIDs, skipped, curRand, session);
            DeleteEntities<Raven>(RavensIDs, skipped, curRand, session);
            DeleteEntities<Peageon>(PeageonsIDs, skipped, curRand, session);

            session.Load<Rock>(RocksIDs.Skip(rand.Next(0, RocksIDs.Count - 1)).First()).Age++;
            session.Store(new Chair
            {
                Name = "Kisse" + skipped,
                Age = skipped
            });
        }

        private static void DefineReplicationFromDeletedToSqlReplication(string sqlReplicationDBName, string deletesSourceDBName, IDocumentStore deletesDB, IDocumentStore sqlReplicationDB)
        {
            IEnumerable<RavenJObject> ravenJObject = new RavenJObject[]{
                            new RavenJObject
                            {
                                { "Url", sqlReplicationDB.Url},
                                { "Database", sqlReplicationDBName }
                            }
                        };
            deletesDB.DatabaseCommands.ForDatabase(deletesSourceDBName).Put(Constants.RavenReplicationDestinations, null, new RavenJObject
                    {
                        {
                            "Destinations", new RavenJArray(ravenJObject)
                        }
                    }, new RavenJObject());
        }

        private static void ResetDBs(IDocumentStore sysStore, string sqlReplicationDBName, string deletesSourceDBName)
        {
            DeleteDB(sysStore, sqlReplicationDBName);
            DeleteDB(sysStore, deletesSourceDBName);

            Thread.Sleep(1000);

            try
            {
                sysStore
                          .DatabaseCommands
                          .GlobalAdmin
                          .CreateDatabase(new DatabaseDocument
                          {
                              Id = sqlReplicationDBName,
                              Settings =
                              {

                            {"Raven/ActiveBundles", "Replication;SqlReplication" },
                            {"Raven/DataDir", $@"C:/temp/embedded2/Databases/{sqlReplicationDBName}"}
                              }
                          });
            }
            catch (Exception)
            {
                Console.WriteLine("DB already exists....");
            }

            try
            {
                sysStore
                          .DatabaseCommands
                          .GlobalAdmin
                          .CreateDatabase(new DatabaseDocument
                          {
                              Id = deletesSourceDBName,
                              Settings =
                              {

                            {"Raven/ActiveBundles", "Replication" },
                            {"Raven/DataDir", $@"C:/temp/embedded2/Databases/{deletesSourceDBName}"}
                              }
                          });
            }
            catch (Exception)
            {
                Console.WriteLine("DB already exists....");
            }
        }

        private static void DeleteDB(IDocumentStore sysStore, string dbName)
        {
            try
            {
                sysStore
                          .DatabaseCommands
                          .GlobalAdmin
                          .DeleteDatabase(dbName, true);

            }
            catch (Exception)
            {
                Console.WriteLine($"DB {dbName} could not be deleted....");
            }
        }

        private static void FillUsersIDs(List<string> UsersIDs, IDocumentSession session)
        {
            var usersStream = session.Advanced.Stream<User>(session.Query<User, UsersByAge>().Where(x => x.Age < 90));

            while (usersStream.MoveNext())
            {
                UsersIDs.Add(usersStream.Current.Key);
            }
        }

        private void DeleteEntities<T>(List<string> ids, int skipped, int curRand, IDocumentSession session)
        {
            var curIdsBatch = ids.Skip(skipped).Take(curRand).ToList();

            foreach (var item in curIdsBatch)
            {
                //     Console.WriteLine($"deleted id {item}");
                var user = session.Load<T>(item);
                session.Delete(user);
                session.SaveChanges();
            }
        }

        private static void GenerateSqlReplicationTasks(Client.IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new SqlReplicationConfig
                {
                    Id = "Raven/SqlReplication/Configuration/Users",
                    Name = "Users",
                    ConnectionString = @"Data Source=karbushim\SQLEXPRESS01;
                        Initial Catalog=People;
                        Integrated Security=SSPI;
                        ",
                    FactoryName = @"System.Data.SqlClient",
                    RavenEntityName = "Users",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable
                        {
                            TableName = "Users", DocumentKeyColumn = "Id"
                        }
                    },
                    Script = @" replicateToUsers({
                                Id:documentId,
                                Name: this.Name,
                                Age: this.Age
                                })",


                });

                session.Store(new SqlReplicationConfig
                {
                    Id = "Raven/SqlReplication/Configuration/Cats",
                    Name = "Cats",
                    ConnectionString = @"Data Source=karbushim\SQLEXPRESS01;
                        Initial Catalog=People;
                        Integrated Security=SSPI;
                        ",
                    FactoryName = @"System.Data.SqlClient",
                    RavenEntityName = "Cats",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable
                        {
                            TableName = "Cats", DocumentKeyColumn = "Id"
                        }
                    },
                    Script = @" replicateToCats({
                                Id:documentId,
                                Name: this.Name,
                                Age: this.Age
                                })"
                });


                session.Store(new SqlReplicationConfig
                {
                    Id = "Raven/SqlReplication/Configuration/Dogs",
                    Name = "Dogs",
                    ConnectionString = @"Data Source=karbushim\SQLEXPRESS01;
                        Initial Catalog=People;
                        Integrated Security=SSPI;
                        ",
                    FactoryName = @"System.Data.SqlClient",
                    RavenEntityName = "Dogs",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable
                        {
                            TableName = "Dogs", DocumentKeyColumn = "Id"
                        }
                    },
                    Script = @" replicateToDogs({
                                Id:documentId,
                                Name: this.Name,
                                Age: this.Age
                                })"
                });

                session.Store(new SqlReplicationConfig
                {
                    Id = "Raven/SqlReplication/Configuration/Horses",
                    Name = "Horses",
                    ConnectionString = @"Data Source=karbushim\SQLEXPRESS01;
                        Initial Catalog=People;
                        Integrated Security=SSPI;
                        ",
                    FactoryName = @"System.Data.SqlClient",
                    RavenEntityName = "Horses",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable
                        {
                            TableName = "Horses", DocumentKeyColumn = "Id"
                        }
                    },
                    Script = @" replicateToHorses({
                                Id:documentId,
                                Name: this.Name,
                                Age: this.Age
                                })"
                });

                session.Store(new SqlReplicationConfig
                {
                    Id = "Raven/SqlReplication/Configuration/Ravens",
                    Name = "Ravens",
                    ConnectionString = @"Data Source=karbushim\SQLEXPRESS01;
                        Initial Catalog=People;
                        Integrated Security=SSPI;
                        ",
                    FactoryName = @"System.Data.SqlClient",
                    RavenEntityName = "Ravens",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable
                        {
                            TableName = "Ravens", DocumentKeyColumn = "Id"
                        }
                    },
                    Script = @" replicateToRavens({
                                Id:documentId,
                                Name: this.Name,
                                Age: this.Age
                                })"
                });

                session.Store(new SqlReplicationConfig
                {
                    Id = "Raven/SqlReplication/Configuration/Peageons",
                    Name = "Peageons",
                    ConnectionString = @"Data Source=karbushim\SQLEXPRESS01;
                        Initial Catalog=People;
                        Integrated Security=SSPI;
                        ",
                    FactoryName = @"System.Data.SqlClient",
                    RavenEntityName = "Peageons",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable
                        {
                            TableName = "Peageons", DocumentKeyColumn = "Id"
                        }
                    },
                    Script = @" replicateToPeageons({
                                Id:documentId,
                                Name: this.Name,
                                Age: this.Age
                                })"
                });

                session.SaveChanges();


            }
        }
    }

    public class MyTest : RavenTest
    {
        [Fact]
        public void DoStuff()
        {
            using (var store = NewDocumentStore())
            {
                Abstractions.Data.Etag firstEtag = null;
                store.DocumentDatabase.TransactionalStorage.Batch(x =>
                {
                    firstEtag = x.Lists.Set("foobar", "init", new RavenJObject(), UuidType.Documents);
                    for (var i=0; i< 10; i++)
                    {
                        x.Lists.Set("foobar", "item" + 0, new RavenJObject(), UuidType.Documents);
                    }
                });


                store.DocumentDatabase.TransactionalStorage.Batch(x =>
                {
                    var results = x.Lists.Read("foobar", firstEtag, null, 10000);
                    foreach (var item in results)
                    {
                        Console.WriteLine(item.Key);
                    }
                });



            }
        }
    }
    public class Program
    {
        public static void Main(string[] args)
        {

            //using (var test = new MyTest())
            //{
            //    test.DoStuff();
            //}
            var test = new DTCSqlReplicationTest();

            test.SqlReplicationTest();



        }
    }
}
