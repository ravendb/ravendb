// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class SetBased : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask
        {
            public override string IndexName => "Index1";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { "from p in docs.patrons select new { p.FirstName }" }
                };
            }
        }

        [Fact]
        public void CanSetPropertyOnArrayItem()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("patrons/1", null,
                    RavenJObject.Parse(
                        @"{
   'Privilege':[
      {
         'Level':'Silver',
         'Code':'12312',
         'EndDate':'12/12/2012'
      }
   ],
   'Phones':[
      {
         'Cell':'123123',
         'Home':'9783041284',
         'Office':'1234123412'
      }
   ],
   'MiddleName':'asdfasdfasdf',
   'FirstName':'asdfasdfasdf'
}"),
                    new RavenJObject
                    {
                        { Constants.Headers.RavenEntityName, "patrons" }
                    });

                new Index1().Execute(store);
                WaitForIndexing(store);

                store
                    .DatabaseCommands
                    .UpdateByIndex(
                        new Index1().IndexName,
                        new IndexQuery { Query = string.Empty },
                        new PatchRequest
                        {
                            Script = "this.Privilege[0].Level = 'Gold'"
                        },
                        options: null)
                    .WaitForCompletion();

                var document = store.DatabaseCommands.Get("patrons/1");

                Assert.Equal("Gold", document.DataAsJson.Value<RavenJArray>("Privilege")[0].Value<string>("Level"));
            }
        }
    }
}
