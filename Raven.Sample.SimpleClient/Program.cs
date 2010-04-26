using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client;

namespace Raven.Sample.SimpleClient
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var documentStore = new DocumentStore("localhost", 8080).Initialise())
            using (var session = documentStore.OpenSession())
            {
                //session.Store(new Company { Name = "Company 1", Region = "A" });
                //session.Store(new Company { Name = "Company 2", Region = "B" });
                //session.SaveChanges();

                var allCompanies = session
                    .Query<Company>("regionIndex")
                    .Where("Region:B")
                    .WaitForNonStaleResults()
                    .ToArray();

                foreach (var company in allCompanies)
                    Console.WriteLine(company.Name);
            }
        }

    }
}