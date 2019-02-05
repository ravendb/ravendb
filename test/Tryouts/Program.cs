using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Xunit;
using Xunit.Sdk;

namespace Tryouts
{
   
    public static class Program
    {
     
        public static void Main(string[] args)
        {
            Console.WriteLine("Paused to allow attachment of WinDBG. Press any key to continue.");
            Console.ReadKey();

            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                DoCoolStuff();
            }
        }

        private static void DoCoolStuff()
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

                    var graphQuery = session.Advanced.GraphQuery<ClientGraphQueries.FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)")
                        .With("Foo", builder => builder.DocumentQuery<ClientGraphQueries.Foo>().WhereIn(x => x.Name, names).WaitForNonStaleResults())
                        .With("Bar", session.Query<ClientGraphQueries.Bar>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age >= 18));

//                    Console.WriteLine(graphQuery);

                    var res = graphQuery
                        .ToList();

                    Console.WriteLine("Mya!!!");

                    Assert.Single(res);
                    Assert.Equal(res[0].Foo.Name, "Foozy");
                    Assert.Equal(res[0].Bar.Name, "Barvazon");
                }
            }
        }
    }
}
