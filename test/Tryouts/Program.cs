//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using FastTests;
//using FastTests.Server.Documents.Queries.Parser;
//using FastTests.Voron.Backups;
//using FastTests.Voron.Compaction;
//using Orders;
//using RachisTests.DatabaseCluster;
using Raven.Client.Documents;
//using Raven.Client.Documents.Commands;
//using Raven.Client.Documents.Queries;
//using Raven.Tests.Core.Utils.Entities;
//using SlowTests.Authentication;
//using SlowTests.Bugs.MapRedue;
//using SlowTests.Client;
//using SlowTests.Client.Attachments;
//using SlowTests.Issues;
//using SlowTests.MailingList;
//using Sparrow.Json;
//using Sparrow.Logging;
//using Sparrow.Threading;
//using StressTests.Client.Attachments;
//using Xunit;
using Employee = Orders.Employee;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            //using (var store = new DocumentStore())
            //{
            //    store.Initialize();
            //    var ids = new List<string>();
            //    using (var session = store.OpenSession())
            //    {
            //        // Length of all the ids together should be larger than 1024 for POST request
            //        for (int i = 0; i < 200; i++)
            //        {
            //            var id = "users/" + i;
            //            ids.Add(id);
            //            session.Store(new User()
            //            {
            //                Name = "Person " + i
            //            }, id);
            //        }
            //        session.SaveChanges();
            //    }
            //    var rq1 = store.GetRequestExecutor();
            //    var cmd = new GetDocumentsCommand(ids.ToArray(), null, true);
            //    using (var ctx = new JsonOperationContext(1024, 1024, SharedMultipleUseFlag.None))
            //    {
            //        rq1.Execute(cmd, ctx);
            //    }
            var store = new DocumentStore
            {
                Urls = new string[]{"http://localhost:8080"},
                Database = "Northwind",
            };
            store.Initialize();
            //using (var session = store.OpenSession())
            //{
            //    var re = session.Advanced.RequestExecutor;
            //    List<string> ids = new List<string>();
            //    for (var i = 100; i >=1 ; --i)
            //    {
            //        ids.Add($"employees/{i}-A");
            //    }
            //    GetDocumentsCommand cmd = new GetDocumentsCommand(ids.ToArray(),null,false);
            //    re.Execute(cmd,session.Advanced.Context);
            //}

            //using (var session = store.OpenSession())
            //{
            //    var re = session.Advanced.RequestExecutor;

            //    GetDocumentsCommand cmd = new GetDocumentsCommand(
            //            new []{ "employees/1-A", "employees/3-A", "employees/3-A", "orders/830-A" }, new[]{ "Employee","ReportsTo"}, false);
            //    re.Execute(cmd, session.Advanced.Context);
            //}

            using (var session = store.OpenSession())
            {
                var employees = session.Include("ReportsTo").Load<Employee>(new[] { "employees/1-A", "employees/2-A" });
            }

            

            //using (var session = store.OpenSession())
            //{
            //    IList<Employee> results = session
            //        .Query<Employee>()
            //        .Where(x => x.FirstName == "Robert")
            //        .ToList();
            //}


        }
    }
}
