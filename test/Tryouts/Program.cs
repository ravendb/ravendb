using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Platform;
using Xunit;

namespace Tryouts
{
   
    public static class Program
    {
        private static void FragmentMemory()
        {
            var rand = new Random();
            var freeMeLater = Marshal.AllocHGlobal(500 * 1024 * 1024);
            while (true)
            {
                try
                {
                    Marshal.AllocHGlobal(rand.Next(1, 256) * 4096);
                }
                catch (OutOfMemoryException oom)
                {
                    Marshal.FreeHGlobal(freeMeLater);
                    return;
                }
            }
        }

        public static unsafe void Main(string[] args)
        {
            Console.WriteLine("Paused to allow attachment of WinDBG. Press any key to continue.");
            Console.ReadKey();

            FragmentMemory();

            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                DoCoolStuffWithGraphs();
            }
        }

        private static void DoCoolStuffWithGraphs()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] {"http://localhost:8080 "},
                Database = "FooBar"
            })
            {
                store.Initialize();
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters
                {
                    DatabaseNames = new[] {"FooBar"},
                    HardDelete = true
                }));
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord("FooBar")));

                using (var session = store.OpenSession())
                {
                    var bar = new ClientGraphQueries.Bar {Name = "Barvazon", Age = 19};
                    var barId = "Bars/1";
                    session.Store(bar, barId);

                    session.Store(new ClientGraphQueries.Foo
                    {
                        Name = "Foozy",
                        Bars = new List<string> {barId}
                    });
                    session.SaveChanges();

                    var names = new[]
                    {
                        "Fi",
                        "Fah",
                        "Foozy"
                    };

                    //var q1 = session.Advanced.DocumentQuery<ClientGraphQueries.Foo>().WhereIn(x => x.Name, names).WaitForNonStaleResults().ToList();
                    //var q2 = session.Query<ClientGraphQueries.Bar>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age >= 18).ToList();

                    //Console.WriteLine($"Q1:{q1.Count}");
                    //Console.WriteLine($"Q2:{q2.Count}");

                    //var graphQuery = session.Advanced.GraphQuery<ClientGraphQueries.FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)")
                    //    .With("Foo", builder => builder.DocumentQuery<ClientGraphQueries.Foo>().WhereIn(x => x.Name, names))
                    //    .With("Bar", session.Query<ClientGraphQueries.Bar>().Where(x => x.Age >= 18)).WaitForNonStaleResults();

                    //Console.WriteLine(graphQuery);

                    var graphQuery = session.Advanced.RawQuery<ClientGraphQueries.FooBar>(@"
                        with {from Foos where Name in ('Fi','Fah','Foozy')} as Foo
                        with {from Bars where Age >= 18} as Bar
                        match (Foo)-[Bars as _]->(Bar)
                    ").WaitForNonStaleResults();

                    //var graphQuery = session.Advanced.RawQuery<ClientGraphQueries.FooBar>(@"
                    //    match (Foos as Foo)-[Bars as _]->(Bars as Bar)
                    //").WaitForNonStaleResults();

                    var res = graphQuery
                        .ToList();

                    Console.WriteLine("Mya!!!");

                    Assert.Single(res);
                    Assert.Equal(res[0].Foo.Name, "Foozy");
                    Assert.Equal(res[0].Bar.Name, "Barvazon");
                }
            }
        }

        private static void DeleteAndCreateDatabase()
        {
            using (var store = new DocumentStore
            {
                Urls = new[] {"http://localhost:8080 "},
                Database = "FooBar"
            })
            {
                store.Initialize();
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters
                {
                    DatabaseNames = new[] {"FooBar"},
                    HardDelete = true
                }));
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord("FooBar")));
            }
        }
    }
}
