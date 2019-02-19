// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
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
                using (var commands = store.Commands())
                {
                    var json = commands.ParseJson(LinuxTestUtils.Dos2Unix(@"{
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
}"));

                    commands.Put("patrons/1", null, json, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "patrons"}
                    });
                }

                new Index1().Execute(store);
                WaitForIndexing(store);

                store
                    .Operations
                    .Send(new PatchByQueryOperation(
                        new IndexQuery { Query = $"FROM INDEX '{new Index1().IndexName}' UPDATE {{ this.Privilege[0].Level = 'Gold' }}" }))
                    .WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var commands = store.Commands())
                {
                    dynamic document = commands.Get("patrons/1");
                    var level = document.Privilege[0].Level.ToString();

                    Assert.Equal("Gold", level);
                }
            }
        }
    }
}
