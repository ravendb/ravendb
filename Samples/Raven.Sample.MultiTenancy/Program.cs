using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Extensions;

namespace Raven.Sample.MultiTenancy
{
    class Program
    {
        static void Main(string[] args)
        {
            using(var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                store.DatabaseCommands.EnsureDatabaseExists("Northwind");


                using(var documentSession = store.OpenSession("Northwind"))
                {
                    documentSession.Store(new { Name = "Ayende"});
                    documentSession.SaveChanges();
                }
            }
        }
    }
}
