using System.Collections.Generic;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
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
                store.Maintenance.Send(new PutIndexesOperation(new[] {  new IndexDefinition
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
                    var json = commands.ParseJson(LinuxTestUtils.Dos2Unix(@"{
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
}"));

                    commands.Put("TestCases/TST00001", null, json, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "TestCases"}
                    });

                    json = commands.ParseJson(@"{
 ""Warnings"": {
   ""AccessoryWarnings"": []
 }
}");

                    commands.Put("TestCases/TST00002", null,
                        json,
                        new Dictionary<string, object>
                        {
                            {Constants.Documents.Metadata.Collection, "TestCases"}
                        });
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
