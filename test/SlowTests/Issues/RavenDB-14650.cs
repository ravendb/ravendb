using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14650 : RavenTestBase
    {
        public RavenDB_14650(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanGetArrayWithType(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var list = new List<int> { 1, 2, 3, 4, 5 };
                var requestExecuter = store.GetRequestExecutor();
                using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["Array"] = new DynamicJsonValue
                        {
                            ["$type"] = "System.Collections.Generic.List`1[[System.Int32, mscorlib]], mscorlib",
                            ["$values"] = new DynamicJsonArray(list.Select(x => (object)x))
                        }
                    }, "users/1");
                    requestExecuter.Execute(new PutDocumentCommand("users/1", null, reader), context);
                }

                var javascriptProjection = @"from @all_docs as doc
select {
     Array: doc.Array
}";
                AssertQuery(javascriptProjection);

                var projection = @"from @all_docs as doc
select doc.Array";
                AssertQuery(projection);

                void AssertQuery(string query)
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Advanced.RawQuery<ArrayClass>(query).ToList();
                        Assert.Equal(1, result.Count);

                        var array = result[0].Array;
                        Assert.NotNull(array);
                        Assert.True(array.SequenceEqual(list));
                    }
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanGetByteArrayWithType(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var requestExecuter = store.GetRequestExecutor();
                using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["ByteArray"] = new DynamicJsonValue
                        {
                            ["$type"] = "System.Byte[], mscorlib",
                            ["$value"] = "AQIDBAU="
                        }
                    }, "users/1");
                    requestExecuter.Execute(new PutDocumentCommand("users/1", null, reader), context);
                }

                var javascriptProjection = @"from @all_docs as doc
select {
     ByteArray: doc.ByteArray
}";
                AssertQuery(javascriptProjection);

                var projection = @"from @all_docs as doc
select doc.ByteArray";
                AssertQuery(projection);

                void AssertQuery(string query)
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Advanced.RawQuery<ByteArrayClass>(query).ToList();
                        Assert.Equal(1, result.Count);

                        var array = result[0].ByteArray;
                        Assert.NotNull(array);
                        Assert.True(array.SequenceEqual(new List<byte> { 1, 2, 3, 4, 5 }));
                    }
                }
            }
        }

        private class ArrayClass
        {
            public List<int> Array { get; set; }
        }

        private class ByteArrayClass
        {
            public Byte[] ByteArray { get; set; }
        }
    }
}
