using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14087 : RavenTestBase
    {
        public RavenDB_14087(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanAssignArrayToPropertyInPatch(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var expectedArray = new List<double> { 40.29394, 26 };

                using (var commands = store.Commands())
                {
                    commands.Put("TestDocuments/1", null, new
                    {
                        StringArray = new string[] { "a", "b" },
                        NumberArray = expectedArray,
                        ObjectArray = new object[] {
                            new
                            {
                                Name = "R"
                            },
                            new
                            {
                                Name = "F"
                            }
                        }
                    },
                    new System.Collections.Generic.Dictionary<string, object>
                    {
                        { Constants.Documents.Metadata.Collection, "TestDocuments" }
                    });
                }

                var operation = store.Operations.Send(new PatchByQueryOperation("from TestDocuments update { this.NewArray = this.NumberArray; }"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var commands = store.Commands())
                {
                    dynamic json = commands.Get("TestDocuments/1");
                    var newArray = ((DynamicArray)json.NewArray).ToList();

                    var doubleArray = newArray
                        .Select(x => (double)x)
                        .ToList();

                    Assert.Equal(expectedArray, doubleArray);
                }
            }
        }
    }
}
