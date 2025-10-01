using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17312 : RavenTestBase
{
    public RavenDB_17312(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void JintPropertyAccessorMustGuaranteeTheOrderOfProperties(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            store.ExecuteIndex(new UsersReducedByNameAndLastName());

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 33 });
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 34});
                
                session.SaveChanges();
                
                Indexes.WaitForIndexing(store);
                
                var results = session.Query<User>("UsersReducedByNameAndLastName").OfType<ReduceResults>().ToList();
                
                Assert.Equal(1, results.Count);

                Assert.Equal(2, results[0].Count);
                Assert.Equal("Joe", results[0].Name);
                Assert.Equal("Doe", results[0].LastName);
            }
        }
    }

    [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void PropertyAccessorMustGuaranteeTheOrderOfPropertiesMultiMapIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            store.ExecuteIndex(new UsersAndEmployeesReducedByNameAndLastNameJs());
            store.ExecuteIndex(new UsersAndEmployeesReducedByNameAndLastNameCSharp());
            store.ExecuteIndex(new UsersAndEmployeesReducedByNameAndLastNameUsingCSharpDictionaries());
            

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 33 });
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 34 });

                session.Store(new Employee { FirstName = "Joe", LastName = "Doe", ReportsTo = null });
                session.Store(new Employee { FirstName = "Joe", LastName = "Doe", ReportsTo = "employees/1-A" });

                session.SaveChanges();

                Indexes.WaitForIndexing(store);

                var resultsFromJsIndex = session.Query<User>("UsersAndEmployeesReducedByNameAndLastNameJs").OfType<ReduceResults>().ToList();

                Assert.Equal(1, resultsFromJsIndex.Count);

                Assert.Equal(4, resultsFromJsIndex[0].Count);
                Assert.Equal("Joe", resultsFromJsIndex[0].Name);
                Assert.Equal("Doe", resultsFromJsIndex[0].LastName);

                var resultsFromCSharpIndex = session.Query<User>("UsersAndEmployeesReducedByNameAndLastNameCSharp").OfType<ReduceResults>().ToList();

                Assert.Equal(1, resultsFromCSharpIndex.Count);

                Assert.Equal(4, resultsFromCSharpIndex[0].Count);
                Assert.Equal("Joe", resultsFromCSharpIndex[0].Name);
                Assert.Equal("Doe", resultsFromCSharpIndex[0].LastName);

                var resultsFromCSharpIndexWithDict = session.Query<User>("UsersAndEmployeesReducedByNameAndLastNameUsingCSharpDictionaries").OfType<ReduceResults>().ToList();

                Assert.Equal(1, resultsFromCSharpIndexWithDict.Count);

                Assert.Equal(4, resultsFromCSharpIndexWithDict[0].Count);
                Assert.Equal("Joe", resultsFromCSharpIndexWithDict[0].Name);
                Assert.Equal("Doe", resultsFromCSharpIndexWithDict[0].LastName);
            }
        }
    }

    [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void WillNotThrowOnJsIndexIfCannotExtractFieldNameFromMapDefinitionButOneFieldWasSpecifiedInOptions(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            store.ExecuteIndex(new UsersReducedByNameAndLastNameResultsPushedToJsArray());

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 33 });
                session.Store(new User { Name = "Joe", LastName = "Doe", Age = 34 });

                session.SaveChanges();

                Indexes.WaitForIndexing(store);

                var results = session.Query<User>("UsersReducedByNameAndLastNameResultsPushedToJsArray").OfType<ReduceResults>().ToList();

                Assert.Equal(1, results.Count);

                Assert.Equal(2, results[0].Count);
                Assert.Equal("Joe", results[0].Name);
                Assert.Equal("Doe", results[0].LastName);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void BackwardCompatibilityWithIndexVersion54001(Options options)
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "54_001_index_ver.ravendb-snapshot");

        using (var file = File.Create(fullBackupPath))
        using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_17312.map-reduce_54_001_index_ver.ravendb-snapshot"))
        {
            stream.CopyTo(file);
        }

        using (var store = GetDocumentStore(options))
        {
            var databaseName = GetDatabaseName();

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = backupPath,
                DatabaseName = databaseName
            }))
            {
                using (var session = store.OpenSession(databaseName))
                {
                    session.Store(new User { Name = "Joe", LastName = "Doe", Age = 31 }, "users/1");
                    session.Store(new User { Name = "Joe", LastName = "Doe", Age = 32 }, "users/2");

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var results = session.Query<User>("UsersReducedByNameAndLastName").OfType<ReduceResults>().ToList();

                    Assert.Equal(1, results.Count);

                    Assert.Equal(2, results[0].Count);
                    Assert.Equal("Joe", results[0].Name);
                    Assert.Equal("Doe", results[0].LastName);

                    results = session.Query<User>("UsersReducedByNameAndLastName_FieldNamesNotExtracted").OfType<ReduceResults>().ToList();

                    Assert.Equal(1, results.Count);

                    Assert.Equal(2, results[0].Count);
                    Assert.Equal("Joe", results[0].Name);
                    Assert.Equal("Doe", results[0].LastName);
                }
            }
        }
    }

    private class ReduceResults
    {
        public string Name { get; set; }

        public string LastName { get; set; }

        public int Count { get; set; }
    }

    private class UsersReducedByNameAndLastName : AbstractJavaScriptIndexCreationTask
    {
        public UsersReducedByNameAndLastName()
        {
            Maps = new HashSet<string>
            {
                // we're forcing here different order of fields of returned results based on Age property

                @"map('Users', function (u){ 
                    
                    if (u.Age % 2 == 0)
                    {
                        return { Count: 1, Name: u.Name, LastName: u.LastName };
                    }

                    return {  LastName: u.LastName, Name: u.Name, Count: 1};
                })",

            };
            Reduce = @"groupBy(x => { return { Name: x.Name, LastName: x.LastName } })
                                .aggregate(g => {return {
                                    Name: g.key.Name,
                                    LastName: g.key.LastName,
                                    Count: g.values.reduce((total, val) => val.Count + total,0)
                               };})";

        }
    }

    private class UsersAndEmployeesReducedByNameAndLastNameJs : AbstractJavaScriptIndexCreationTask
    {
        public UsersAndEmployeesReducedByNameAndLastNameJs()
        {
            Maps = new HashSet<string>
            {
                // we're forcing here different order of fields of returned results based on Age property

                @"map('Users', function (u){ 
                    
                    if (u.Age % 2 == 0)
                    {
                        return { Count: 1, Name: u.Name, LastName: u.LastName };
                    }

                    return {  LastName: u.LastName, Name: u.Name, Count: 1};
                })",

                // we're forcing here different order of fields of returned results based on ReportsTo property

                @"map('Employees', function (e){ 
                    
                    if (e.ReportsTo == null)
                    {
                        return { Count: 1, Name: e.FirstName, LastName: e.LastName };
                    }

                    return {  LastName: e.LastName, Name: e.FirstName, Count: 1};
                })",
            };
            Reduce = @"groupBy(x => { return { Name: x.Name, LastName: x.LastName } })
                                .aggregate(g => {return {
                                    Name: g.key.Name,
                                    LastName: g.key.LastName,
                                    Count: g.values.reduce((total, val) => val.Count + total,0)
                               };})";

        }
    }

    private class UsersAndEmployeesReducedByNameAndLastNameCSharp : AbstractMultiMapIndexCreationTask<ReduceResults>
    {
        public UsersAndEmployeesReducedByNameAndLastNameCSharp()
        {
            AddMap<User>(users => from u in users select new
            {
                Count = 1,
                Name = u.Name,
                u.LastName,
            });

            AddMap<Employee>(employees => from e in employees select new
            {
                e.LastName,
                Name = e.FirstName,
                Count = 1
            });

            Reduce = results => from r in results group r by new { r.Name, r.LastName } into g select new
            {
                g.Key.Name,
                g.Key.LastName,
                Count = g.Sum(x => x.Count)
            };
        }
    }

    private class UsersAndEmployeesReducedByNameAndLastNameUsingCSharpDictionaries : AbstractMultiMapIndexCreationTask<ReduceResults>
    {
        public UsersAndEmployeesReducedByNameAndLastNameUsingCSharpDictionaries()
        {
            AddMap<User>(users => from u in users
                select new Dictionary<string, object>
                {
                    { "Count", 1 },
                    { "Name", u.Name },
                    { "LastName", u.LastName },
                });

            AddMap<Employee>(employees => from e in employees
                select new Dictionary<string, object>
                {
                    { "LastName", e.LastName },
                    { "Name", e.FirstName },
                    { "Count", 1 },
                });

            Reduce = results => from r in results
                group r by new { r.Name, r.LastName } into g
                select new
                {
                    g.Key.Name,
                    g.Key.LastName,
                    Count = g.Sum(x => x.Count)
                };
        }
    }

    private class UsersReducedByNameAndLastNameResultsPushedToJsArray : AbstractJavaScriptIndexCreationTask
    {
        public UsersReducedByNameAndLastNameResultsPushedToJsArray()
        {
            Maps = new HashSet<string>
            {
                // we're forcing here different order of fields of returned results based on Age property

                @"map('Users', function (u){ 
                    
                    var res = [];
                    
                    if (u.Age % 2 == 0)
                    {
                        res.push({ Count: 1, Name: u.Name, LastName: u.LastName });
                    }
                    else
                    {
                        res.push({ LastName: u.LastName, Name: u.Name, Count: 1});
                    }

                    return res;
                })",

            };
            Reduce = @"groupBy(x => { return { Name: x.Name, LastName: x.LastName } })
                                .aggregate(g => {return {
                                    Name: g.key.Name,
                                    LastName: g.key.LastName,
                                    Count: g.values.reduce((total, val) => val.Count + total,0)
                               };})";

            Fields = new Dictionary<string, IndexFieldOptions>
            {
                {"LastName", new IndexFieldOptions() {Indexing = FieldIndexing.Search}}
            };
        }
    }
}
