using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client;
using Raven.Database.Indexing;

namespace Raven.Sample.SimpleClient
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var documentStore = new DocumentStore { Url = "http://localhost:8080" })
            {
                documentStore.Initialize();

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Company { Name = "Company 1", Region = "Asia" });
                    session.Store(new Company { Name = "Company 2", Region = "Africa" });
                    session.SaveChanges();

                    var africanCompanies = session.Query<Company>()
                        .Customize(x=>x.WaitForNonStaleResultsAsOfNow())
                        .Where(x=>x.Region == "Africa")
                        .ToArray();

                    foreach (var company in africanCompanies)
                        Console.WriteLine(company.Name);
                }
            }
        }

    }
}
