using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Client;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using SlowTests.Client.Attachments;
using SlowTests.Core.Session;
using SlowTests.SlowTests.Issues;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var store = new DocumentStore
            {
                Url = "http://localhost.fiddler:8080",
                DefaultDatabase = "Tasks"
            };

            store.Initialize();

            using (var session = store.OpenSession())
            {
                var task = new ToDoTask
                {
                    DueDate = DateTime.Today.AddDays(1),
                    Task = "Buy milk"
                };
                session.Store(task);
                session.SaveChanges();
            }
        }
    }



    public class ToDoTask
    {
        public string Id { get; set; }
        public string Task { get; set; }
        public bool Completed { get; set; }
        public DateTime DueDate { get; set; }
    }
}