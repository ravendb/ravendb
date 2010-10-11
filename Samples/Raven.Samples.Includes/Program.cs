using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Samples.Includes
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using (var documentStore = new DocumentStore {Url = "http://localhost:8080"})
            {
                documentStore.Initialize();
            
                IndexCreation.CreateIndexes(typeof(Companies_ByRegion).Assembly, documentStore);

                using (IDocumentSession session = documentStore.OpenSession())
                {
                    var company = session.Include<Company>(x => x.ParentCompanyId)
                        .Load("companies/2");

                    var parent = session.Load<Company>(company.ParentCompanyId);

                    Console.WriteLine(company.Name);
                    Console.WriteLine(parent.Name);
                }
            }
        }
    }
}
