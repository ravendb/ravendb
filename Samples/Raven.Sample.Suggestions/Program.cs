//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Sample.Suggestions
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                IndexCreation.CreateIndexes(typeof(Users_ByName).Assembly, store);

                using (var session = store.OpenSession())
                {
                    foreach (var name in new[] { "Oren Eini", "Ayende Rahien" })
                    {
                        session.Store(new User
                        {
                            Name = name
                        });
                    }

                    session.SaveChanges();
                }

                while (true)
                {
                    using (var session = store.OpenSession())
                    {
                        Console.Write("Enter user name: ");
                        var name = Console.ReadLine();

                        PerformQuery(session, name);
                    }
                }


            }
        }

        private static void PerformQuery(IDocumentSession session, string name)
        {
            var q = from user in session.Query<User>("Users/ByName")
                    where user.Name == name
                    select user;

            var foundUser = q.FirstOrDefault();

            if(foundUser == null)
            {
                var suggestionQueryResult = q.Suggest();
                if(suggestionQueryResult.Suggestions.Length==1)
                {
                    PerformQuery(session, suggestionQueryResult.Suggestions[0]);
                    return;          
                }
                Console.WriteLine("Did you mean?");
                foreach (var suggestion in suggestionQueryResult.Suggestions)
                {
                    Console.WriteLine("\t{0}", suggestion);
                }

            }
            else
            {
                Console.WriteLine("Found user: {0}", foundUser.Id);
            }
        }
    }

    public class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from user in users
                           select new {user.Name};
            Indexes.Add(x=>x.Name, FieldIndexing.Analyzed);
        }
    }

    public class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
