using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Linq;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using Xunit;

namespace FastTests.Server.Basic
{
    public class ServerStore : RavenTestBase
    {
        [Fact]
        public void Server_store_basic_read_write_should_work()
        {
            using (GetDocumentStore())
            {
                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar"
                    };

                    Server.ServerStore.Write(context, "foo/bar", context.ReadObject(foo, "read test stuff"));
                    tx.Commit();
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {

                    var fetched = Server.ServerStore.Read(context, "foo/bar");
                    string val;
                    Assert.True(fetched.TryGet("Foo",out val));
                    Assert.Equal("Bar", val);
                }

            }
        }

        [Fact]
        public void Server_store_write_should_throw_concurrency_exception_if_relevant()
        {
            using (GetDocumentStore())
            {
                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar"
                    };

                    using (var obj = context.ReadObject(foo, "read test stuff"))
                    {
                        Server.ServerStore.Write(context, "foo/bar", obj);
                        tx.Commit();
                    }
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar2"
                    };

                    using (var obj = context.ReadObject(foo, "read test stuff"))
                    {
                        Server.ServerStore.Write(context, "foo/bar", obj);
                        tx.Commit();
                    }
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (context.OpenWriteTransaction())
                {

                    var foo = new DynamicJsonValue
                    {
                        ["Foo"] = "Bar3"
                    };

                    using (var blittableObj = context.ReadObject(foo, "read test stuff"))
                    {
                        //this shouldn't throw, since expected etag == null
                        Server.ServerStore.Write(context, "foo/bar", blittableObj);

                        var lastEtag = Server.ServerStore.ReadLastEtag(context);
                        //this shouldn't throw, since expected etag == existing etag
                        Server.ServerStore.Write(context, "foo/bar", blittableObj, lastEtag);

                        //this should throw because existing etag doesn't match with existing etag
                        Assert.Throws<ConcurrencyException>(
                            () => Server.ServerStore.Write(context, "foo/bar", blittableObj, 1));

                        //this should throw because it has expected etag, but there is no existing value
                        Assert.Throws<ConcurrencyException>(
                            () => Server.ServerStore.Write(context, "foo/bar2", blittableObj, 1));
                    }
                }

            }
        }
    }
}
