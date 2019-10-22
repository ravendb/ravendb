using FastTests;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5570 : RavenLowLevelTestBase
    {
        public RavenDB_5570(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Doing_PUT_without_commit_should_not_cause_NRE_on_subsequent_PUTs()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var doc = context.ReadObject(new DynamicJsonValue
                {
                    ["Foo"] = "Bar"
                }, "RavenDB-5570 Test"))
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
