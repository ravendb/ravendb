using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;

using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_514 : RavenTestBase
    {
         [Fact]
         public void BoostWithLinq()
         {
             using(var store = GetDocumentStore())
             {
                 store.DatabaseCommands.PutIndex("test", new IndexDefinition
                 {
                     Maps = { "from p in docs.Products select new { p.Price} .Boost(2)" }
                 });
             }
         }

         [Fact]
         public void BoostWithMethod()
         {
             using (var store = GetDocumentStore())
             {
                 store.DatabaseCommands.PutIndex("test", new IndexDefinition
                 {
                     Maps = { "docs.Products.Select(p =>new { p.Price } .Boost(2))" }
                 });
             }
         }
    }
}
