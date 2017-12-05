using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace HelloWorld
{
    internal static class Program
    {
        private const string DatabaseName = "MyFirstDB";

        private class User
        {
            public string Name;
            public string Domain;
            public DateTime LastLogin;
        }
        
        private class DomainFullTextSearch : AbstractIndexCreationTask<User>
        {
            public DomainFullTextSearch()
            {
                Map = users => from user in users
                    select new
                    {
                        Name = user.Name,
                        Domain = user.Domain,
                        LastLogin = user.LastLogin
                    };
                
                Indexes.Add(x => x.Domain, FieldIndexing.Search);
                Indexes.Add(x => x.LastLogin, FieldIndexing.Search);
            }
            
            
        }

        private static void Main()
        {
            using (var documentStore = new DocumentStore
            {
                Urls = new[] {"http://localhost:50000"},
                Database = DatabaseName
            })
            {           
                documentStore.Initialize();

                var dbRecord = documentStore.Admin.Server.Send(new GetDatabaseRecordOperation(DatabaseName));
                if (dbRecord == null)
                {
                    documentStore.Admin.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(DatabaseName)));

                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "Laura",
                            Domain = "example.net",
                            LastLogin = DateTime.Today
                        });

                        session.Store(new User
                        {
                            Name = "Robert",
                            Domain = "example.com",
                            LastLogin = DateTime.Today - TimeSpan.FromDays(1)
                        });

                        session.Store(new User
                        {
                            Name = "Margaret",
                            Domain = "example.net",
                            LastLogin = DateTime.Today - TimeSpan.FromDays(2)
                        });

                        session.Store(new User
                        {
                            Name = "Andrew",
                            Domain = "example.com",
                            LastLogin = DateTime.Today - TimeSpan.FromDays(3)
                        });

                        session.SaveChanges();
                    }
                }
                
                documentStore.ExecuteIndex(new DomainFullTextSearch());                
                
                using (var session = documentStore.OpenSession())
                {
                    var newLoginsDate = DateTime.Today - TimeSpan.FromDays(1);
                    
                    var oldLogins = session.Query<User>()
                        .Where(user => user.LastLogin < newLoginsDate).Select(x => x.Name).ToList();
                    var dotcomUsers = session.Query<User, DomainFullTextSearch>().Customize(y => y.WaitForNonStaleResults())
                        .Search(x => x.Domain, "example.com").ToList()
                        .Select(y => $"{y.Name.ToLowerInvariant()}@{y.Domain}").ToList();                        
                    var newLoginsToDotNet = session.Query<User, DomainFullTextSearch>()
                        .Search(x => x.Domain, "example.net").Where(user => user.LastLogin > newLoginsDate).ToList()
                        .Select(y => $"{y.Name.ToLowerInvariant()}@{y.Domain}").ToList();
                        
                    
                    const int pad = 28;
                    Console.WriteLine($"+{new string('-', pad-1)}+{new string('-', pad*2)}+");
                    Console.WriteLine("| Query".PadRight(pad) + "| Results".PadRight(pad*2)+" |");
                    Console.WriteLine($"+{new string('-', pad-1)}+{new string('-', pad*2)}+");
                    Console.WriteLine("| Old Logins".PadRight(pad) + $"| {string.Join(',', oldLogins).PadRight(pad*2-1)}|");
                    Console.WriteLine("| .com Emails".PadRight(pad) + $"| {string.Join(',', dotcomUsers).PadRight(pad*2-1)}|");
                    Console.WriteLine("| New Logins of .net Emails".PadRight(pad) + $"| {string.Join(',', newLoginsToDotNet).PadRight(pad*2-1)}|");
                    Console.WriteLine($"+{new string('-', pad-1)}+{new string('-', pad*2)}+");
                }
            }
        }
    }
}


