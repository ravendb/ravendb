//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database;

namespace Raven.Sample.Replication
{
    class Program
    {
        static void Main()
        {
			var documentStore1 = new DocumentStore { Url = "http://localhost:8080" }.Initialize();
			var documentStore2 = new DocumentStore { Url = "http://localhost:8081" }.Initialize();
           
            using(var session1 = documentStore1.OpenSession())
            {
                session1.Store(new User { Id = "users/ayende", Name = "Ayende" });
                session1.SaveChanges();
            }

            using (var session2 = documentStore2.OpenSession())
            {
                session2.Store(new User { Id = "users/ayende", Name = "Oren" });
                session2.SaveChanges();
            }
            Console.WriteLine("Conflicted documents set up");
            Console.WriteLine("Please setup replicaton now...");
            Console.ReadLine();

            using (var session2 = documentStore2.OpenSession())
            {
                try
                {
                    session2.Load<User>("users/ayende");
                }
                catch (ConflictException e)
                {
                    Console.WriteLine("Choose which document you want to preserve:");
                    var list = new List<JsonDocument>();
                    for (int i = 0; i < e.ConflictedVersionIds.Length; i++)
                    {
                        var doc = documentStore2.DatabaseCommands.Get(e.ConflictedVersionIds[i]);
                        list.Add(doc);
                        Console.WriteLine("{0}. {1}", i, doc.DataAsJson.ToString(Formatting.None));
                    }
                    var select = int.Parse(Console.ReadLine());
                    var resolved = list[select];
                    documentStore2.DatabaseCommands.Put("users/ayende", null, resolved.DataAsJson, resolved.Metadata);
                }
            }

            Console.WriteLine("Conflict resolved...");
            Console.ReadLine();

            using (var session2 = documentStore2.OpenSession())
            {
                var user = session2.Load<User>("users/ayende");
                Console.WriteLine(user.Name);
                user.Name = "Ayende Rahien";
                session2.SaveChanges();
            }
        }
    }

    public class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
