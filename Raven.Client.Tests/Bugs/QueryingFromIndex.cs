using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Tests.Document;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class QueryingFromIndex : LocalClientTest
    {
        [Fact]
        public void LuceneQueryWithIndexIsCaseInsensitive()
        {
            using (var store = this.NewDocumentStore())
            {
                var definition = new IndexDefinition<Company>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Name
                                  }
                }.ToIndexDefinition(store.Conventions);
                store.DatabaseCommands.PutIndex("CompanyByName",
                                                definition);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Google" });
                    session.Store(new Company
                    {
                        Name =
                            "HibernatingRhinos"
                    });
                    session.SaveChanges();

                    var company =
                        session.Advanced.LuceneQuery<Company>("CompanyByName")
                            .Where("Name:Google")
                            .WaitForNonStaleResults()
                            .FirstOrDefault();

                    Assert.NotNull(company);
                }
            }
        }

        [Fact]
        public void LinqQueryWithIndexIsCaseInsensitive()
        {
            using (var store = this.NewDocumentStore())
            {
                var definition = new IndexDefinition<Company>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Name
                                  }
                }.ToIndexDefinition(store.Conventions);
                store.DatabaseCommands.PutIndex("CompanyByName",
                                                definition);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Google" });
                    session.Store(new Company
                    {
                        Name =
                            "HibernatingRhinos"
                    });
                    session.SaveChanges();

                    var company =
                        session.Query<Company>("CompanyByName")
                            .Customize(x=>x.WaitForNonStaleResults())
                            .Where(x=>x.Name == "Google")
                            .FirstOrDefault();

                    Assert.NotNull(company);
                }
            }
        }
    }
}