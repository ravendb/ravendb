using FastTests;
using Raven.Client.Data;
using Xunit;

namespace SlowTests.MailingList
{
    public class WildCardQuery : RavenTestBase
    {
         [Fact]
         public void CanQuery()
         {
             using(var store = GetDocumentStore())
             {
                 store.DatabaseCommands.Query("dynamic", new IndexQuery
                 {
                     Query = "PortalId:0 AND Query:(*) QueryBoosted:(*)"
                 });
             }
         }
    }
}
