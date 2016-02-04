using System.Dynamic;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs.Async
{
   public class DynamicGeneratedIds : RavenTest
   {
      [Fact]
      public void AsyncMatchesSyncGeneratedIdForDynamicBehavior()
      {
         using (var store = NewDocumentStore())
         {
            using (var session = store.OpenAsyncSession())
            {
               dynamic client = new ExpandoObject();
               client.Name = "Test";
               var result = session.StoreAsync(client);
               result.Wait();

               Assert.Equal("ExpandoObjects/1", client.Id);
            }
         }
      }

      [Fact]
      public void GeneratedIdForDynamicTagNameAsync()
      {
         using (var store = NewDocumentStore())
         {
            store.Conventions.FindDynamicTagName = (entity) => entity.EntityName;

            using (var session = store.OpenAsyncSession())
            {
               dynamic client = new ExpandoObject();
               client.Name = "Test";
               client.EntityName = "clients";

               var result = session.StoreAsync(client);
               result.Wait();

               Assert.Equal("clients/1", client.Id);
            }
         }
      }
   }
}
