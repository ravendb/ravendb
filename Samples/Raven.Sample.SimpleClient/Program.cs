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
            	documentStore.Initialise();
				//documentStore.DatabaseCommands.PutIndex("regionIndex",
				//                                        new IndexDefinition
				//                                        {
				//                                            Map = "from company in docs.Companies select new{company.Region}"
				//                                        });
				using (var session = documentStore.OpenSession())
            {
				

				session.Store(new Company { Name = "Company 1", Region = "Asia" });
				session.Store(new Company { Name = "Company 2", Region = "Africa" });
				session.SaveChanges();

                var allCompanies = session
                    .Query<Company>("regionIndex")
                    .Where("Region:Africa")
                    .WaitForNonStaleResults()
                    .ToArray();

                foreach (var company in allCompanies)
                    Console.WriteLine(company.Name);
            }}
        }

    }
}