using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using SlowTests.Client.Attachments;
using SlowTests.Tests.Sorting;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace Tryouts
{
    public class AlphaNumericSortingTest : RavenTestBase
    {        
        public class BigNumbersIndex:AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = new System.Collections.Generic.HashSet<string>
                    {
                        @"from doc in docs.BigNumbers
                           select new { Number = doc.Number}"
                    }
                };
            }
        }
        [Fact]
        public void ShouldSortBigAlphanumericNumbers()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecuter = store.GetRequestExecutor();
                using (var session = store.OpenSession() as InMemoryDocumentSessionOperations)
                {
                    using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                    {
                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                            context.ReadObject(new DynamicJsonValue
                            {
                                ["Number"] = new LazyNumberValue(context.GetLazyString("3.141592653589793"))
                                ,
                                ["@metadata"] = new DynamicJsonValue
                                {
                                    ["@collection"] = "numbers"
                                }
                            }, "pi")), context);

                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                            context.ReadObject(new DynamicJsonValue
                            {
                                ["Number"] = new LazyNumberValue(context.GetLazyString("1e40"))
                                ,
                                ["@metadata"] = new DynamicJsonValue
                                {
                                    ["@collection"] = "numbers"
                                }
                            }, "pi")), context);

                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                            context.ReadObject(new DynamicJsonValue
                            {
                                ["Number"] = new LazyNumberValue(context.GetLazyString("19"))
                                ,
                                ["@metadata"] = new DynamicJsonValue
                                {
                                    ["@collection"] = "numbers"
                                }
                            }, "pi")), context);

                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                           context.ReadObject(new DynamicJsonValue
                           {
                               ["Number"] = new LazyNumberValue(context.GetLazyString("1.9e+21"))
                               ,
                               ["@metadata"] = new DynamicJsonValue
                               {
                                   ["@collection"] = "numbers"
                               }
                           }, "pi")), context);

                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                           context.ReadObject(new DynamicJsonValue
                           {
                               ["Number"] = new LazyNumberValue(context.GetLazyString("1994"))
                               ,
                               ["@metadata"] = new DynamicJsonValue
                               {
                                   ["@collection"] = "numbers"
                               }
                           }, "pi")), context);

                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                           context.ReadObject(new DynamicJsonValue
                           {
                               ["Number"] = new LazyNumberValue(context.GetLazyString("18.997"))
                               ,
                               ["@metadata"] = new DynamicJsonValue
                               {
                                   ["@collection"] = "numbers"
                               }
                           }, "pi")), context);

                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                           context.ReadObject(new DynamicJsonValue
                           {
                               ["Number"] = new LazyNumberValue(context.GetLazyString("1.999999999999999"))
                               ,
                               ["@metadata"] = new DynamicJsonValue
                               {
                                   ["@collection"] = "numbers"
                               }
                           }, "pi")), context);

                        requestExecuter.Execute(new PutDocumentCommand("numbers/", null,
                           context.ReadObject(new DynamicJsonValue
                           {
                               ["Number"] = new LazyNumberValue(context.GetLazyString("1.9e+200"))
                               ,
                               ["@metadata"] = new DynamicJsonValue
                               {
                                   ["@collection"] = "numbers"
                               }
                           }, "pi")), context);


                        QueryCommand queryCommand = new QueryCommand(session, new Raven.Client.Documents.Queries.IndexQuery
                        {
                            Query = @"
                                from numbers as n
                                order by n.Number as alphanumeric desc"
                        });
                        requestExecuter.Execute(
                     queryCommand, context, null);

                        foreach (var item in queryCommand.Result.Results)
                        {
                            System.Console.WriteLine((item as BlittableJsonReaderObject)["Number"]);
                        }
                        

                    }
                }

                
            }
        }
    }
    public static class Program
    {


        public static void Main(string[] args)
        {
        }
    }
}
