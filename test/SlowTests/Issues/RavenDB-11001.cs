using FastTests;
using Raven.Client.Documents.Commands;
using Xunit;
using Sparrow.Json.Parsing;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace SlowTests.Issues
{
    public class RavenDB_11001:RavenTestBase
    {
        [Fact]
        public void JavascriptProjectionOfMapOfArrayWithNonexistingFieldShouldReturnArrayOfNulls()
        {
            using (var store = GetDocumentStore())                
            {
                var executer = store.GetRequestExecutor();

                using (executer.ContextPool.AllocateOperationContext(out var context))
                {
                    var docBlit = context.ReadObject(new DynamicJsonValue
                    {
                        ["InnerArray"] = new[]
                        {
                            new DynamicJsonValue(),
                            new DynamicJsonValue()
                        },
                        [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "docs"
                        }
                    }, "newDoc");
                    store.Commands().Execute(new PutDocumentCommand("docs/1", null, docBlit));

                    using (var session = store.OpenSession())
                    {
                        QueryCommand queryCommand = new QueryCommand(session as InMemoryDocumentSessionOperations, new IndexQuery()
                        {
                            Query = "from docs as d select { inner:d.InnerArray.map(x=>x.f)}"
                        });
                        store.Commands().Execute(queryCommand);
                        var resBlittable = queryCommand.Result.Results[0] as BlittableJsonReaderObject;
                        var inner = resBlittable["inner"] as BlittableJsonReaderArray;
                        Assert.Equal(2, inner.Length);
                        for (var i=0; i< 2; i++)
                        {
                            Assert.Equal(null, inner[i]);
                        }
                    }                
                }
            }
        }


        [Fact]
        public void JavascriptProjectionOfMapOfArrayWithNonexistingFieldWrappedWithObjectShouldReturnArrayOfObjectsWithNulls()
        {
            using (var store = GetDocumentStore())
            {
                var executer = store.GetRequestExecutor();

                using (executer.ContextPool.AllocateOperationContext(out var context))
                {
                    var docBlit = context.ReadObject(new DynamicJsonValue
                    {
                        ["InnerArray"] = new[]
                        {
                            new DynamicJsonValue(),
                            new DynamicJsonValue()
                        },
                        [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "docs"
                        }
                    }, "newDoc");
                    store.Commands().Execute(new PutDocumentCommand("docs/1", null, docBlit));

                    using (var session = store.OpenSession())
                    {
                        QueryCommand queryCommand = new QueryCommand(session as InMemoryDocumentSessionOperations, new IndexQuery()
                        {
                            Query = "from docs as d select { inner:d.InnerArray.map(x=>({f:x.f}))}"
                        });
                        store.Commands().Execute(queryCommand);
                        var resBlittable = queryCommand.Result.Results[0] as BlittableJsonReaderObject;
                        var inner = resBlittable["inner"] as BlittableJsonReaderArray;
                        Assert.Equal(2, inner.Length);
                        for (var i = 0; i < 2; i++)
                        {
                            var innerObject = inner[i] as BlittableJsonReaderObject;
                            Assert.Equal(null, innerObject["f"]);
                        }
                    }
                }
            }
        }


        [Fact]
        public void RQLProjectionOfMapOfArrayWithNonexistingFieldShouldReturnArrayOfNulls()
        {
            using (var store = GetDocumentStore())
            {
                var executer = store.GetRequestExecutor();

                using (executer.ContextPool.AllocateOperationContext(out var context))
                {
                    var docBlit = context.ReadObject(new DynamicJsonValue
                    {
                        ["InnerArray"] = new[]
                        {
                            new DynamicJsonValue(),
                            new DynamicJsonValue()
                        },
                        [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "docs"
                        }
                    }, "newDoc");
                    store.Commands().Execute(new PutDocumentCommand("docs/1", null, docBlit));

                    using (var session = store.OpenSession())
                    {
                        QueryCommand queryCommand = new QueryCommand(session as InMemoryDocumentSessionOperations, new IndexQuery()
                        {
                            Query = "from docs select InnerArray[].f"
                        });
                        store.Commands().Execute(queryCommand);
                        var resBlittable = queryCommand.Result.Results[0] as BlittableJsonReaderObject;
                        var inner = resBlittable["InnerArray[].f"] as BlittableJsonReaderArray;
                        Assert.Equal(2, inner.Length);
                        for (var i = 0; i < 2; i++)
                        {
                            Assert.Equal(null, inner[i]);
                        }
                    }
                }
            }
        }
    }
}
