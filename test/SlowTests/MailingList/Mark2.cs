using FastTests;
using Raven.Abstractions.Data;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using SlowTests.Utils;
using Xunit;

namespace SlowTests.MailingList
{
    public class Mark2 : RavenTestBase
    {
        [Fact]
        public void ShouldNotGetErrors()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test", new IndexDefinition
                {
                    Maps = { @"from brief in docs.TestCases
 select new {
 _tWarnings_AccessoryWarnings_Value = brief.Warnings.AccessoryWarnings.Select(y=>y.Value)
 }"
}
                });

                store.DatabaseCommands.Put("TestCases/TST00001", null,
                                           RavenJObject.Parse(
                                            @"{
 ""Warnings"": {
   ""AccessoryWarnings"": [
     {
       ""Value"": ""whatever"",
       ""Id"": 123
     },
     {
       ""Value"": ""dsfsdfsd sfsd sd"",
       ""Id"": 1234
     }
   ]
 }
}"),
                                           new RavenJObject { { Constants.Headers.RavenEntityName, "TestCases" } });

                store.DatabaseCommands.Put("TestCases/TST00002", null,
                                           RavenJObject.Parse(
                                            @"{
 ""Warnings"": {
   ""AccessoryWarnings"": []
 }
}"),
                                           new RavenJObject { { Constants.Headers.RavenEntityName, "TestCases" } });

                WaitForIndexing(store);

                TestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
