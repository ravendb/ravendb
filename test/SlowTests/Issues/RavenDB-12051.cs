using System.Collections.Generic;
using System.IO;
using FastTests;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12051 : RavenTestBase
    {
        [Fact]
        public void CompareBlittableShouldNotFailWhenOldPropIsArrayAndNewPropIsObjectWithTypeAndCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var stringStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("{'Items': []}")))
                using (var blittableJson = context.Read(stringStream, "Reading of foo/bar"))
                {
                    store.GetRequestExecutor().Execute(new PutDocumentCommand("foo/bar", null, blittableJson), context);
                }
                using (var s = store.OpenSession())
                {
                    s.Load<User>("foo/bar");
                    s.SaveChanges();
                }
            }
        }

        private class User
        {
            public IEnumerable<string> Items => GetItems();

            private IEnumerable<string> GetItems()
            {
                yield break;
            }
        }


    }
}
