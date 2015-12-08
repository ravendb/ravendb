using System;
using System.Diagnostics;
using Raven.Client.Embedded;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main()
        {
            using (var store = new EmbeddableDocumentStore
            {
                UseEmbeddedHttpServer = true,
                DefaultDatabase = "FooBar"
            })
            {
                store.Initialize();

                store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("FooBar");
                Console.ReadLine();
            }
        }
}
    }
