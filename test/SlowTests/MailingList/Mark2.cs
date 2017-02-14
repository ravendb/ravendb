using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Utils;
using Xunit;

namespace SlowTests.MailingList
{
    public class Mark2 : RavenNewTestBase
    {
        [Fact]
        public void ShouldNotGetErrors()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new[] {  new IndexDefinition
                {
                    Name = "test", 
                    Maps = { @"from brief in docs.TestCases
 select new {
 _tWarnings_AccessoryWarnings_Value = brief.Warnings.AccessoryWarnings.Select(y=>y.Value)
 }"
}
                }}));

                using (var commands = store.Commands())
                {
                    var json = commands.ParseJson(@"{
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
}");

                    commands.Put("TestCases/TST00001", null, json, new Dictionary<string, string>
                    {
                        {Constants.Metadata.Collection, "TestCases"}
                    });

                    json = commands.ParseJson(@"{
 ""Warnings"": {
   ""AccessoryWarnings"": []
 }
}");

                    commands.Put("TestCases/TST00002", null,
                        json,
                        new Dictionary<string, string>
                        {
                            {Constants.Metadata.Collection, "TestCases"}
                        });
                }

                WaitForIndexing(store);

                TestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
