using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5570 : RavenLowLevelTestBase
    {
        [Fact]
        public void Doing_PUT_without_commit_should_not_cause_NRE_on_subsequent_PUTs()
        {
            using (var database = CreateDocumentDatabase())
            {
                DocumentsOperationContext context;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (var doc = context.ReadObject(new DynamicJsonValue
                {
                    ["Foo"] = "Bar"
                },"RavenDB-5570 Test"))
                {
                    //do PUT, but do not commit the transaction
                    using (context.OpenWriteTransaction())
                    {
                        database.DocumentsStorage.Put(context, "foo/bar", null, doc);
                    }

                    using (context.OpenWriteTransaction())
                    {
                        //should not throw exception...
                        database.DocumentsStorage.Put(context, "foo/bar", null, doc);
                    }
                }
            }
        }
    }
}
