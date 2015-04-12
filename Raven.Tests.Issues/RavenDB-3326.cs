using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3326 : RavenTestBase
    {
        [Fact]
        public void streaming_and_projections_with_property_rename()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true))
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer {Name = "John", Address = "Tel Aviv"});
					session.Store(new Customer { Name = "John2", Address = "Tel Aviv2" });
                    session.SaveChanges();
          
                    WaitForIndexing(store);

                   // WaitForUserToContinueTheTest(store);
/*

					var list = session.Query<Customer>(index.IndexName)
						.Select(r => new
						{
							Name = r.Name,
							OtherThanName = r.Address
						}).ToList();
*/

					  var query = session.Query<Customer>(index.IndexName)
                        .Select(r => new
                        {
                            Name = r.Name,
                            OtherThanName = r.Address
                        });

                    
                        //WaitForUserToContinueTheTest(store);
             
                        var enumerator = session.Advanced.Stream(query);

                        while (enumerator.MoveNext())
                        {
                            var result = enumerator.Current.Document.Name; //works
                            var result2 = enumerator.Current.Document.OtherThanName; //Null
                        }
                    }
                }
            }

        public class Customer
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }

        public class Customers_ByName : AbstractIndexCreationTask<Customer>
        {
            public Customers_ByName()
            {
                Map = customers => from customer in customers
                                   select new
                                   {
                                       customer.Name
                                   };
            }
        }
}
}
 

