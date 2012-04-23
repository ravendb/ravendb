// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class SetBased : RavenTest
	{
		[Fact]
		public void CanSetPropertyOnArrayItem()
		{
			using (var store = NewDocumentStore())
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
				                           	{Constants.RavenEntityName, "patrons"}
				                           });

				using(var session = store.OpenSession())
				{
					session.Query<object>("Raven/DocumentsByEntityName")
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
				}

				store.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName",
				                                     new IndexQuery {Query = "Tag:patrons"},
				                                     new[]
				                                     {
				                                     	new PatchRequest
				                                     	{
				                                     		Type = PatchCommandType.Modify,
				                                     		Name = "Privilege",
				                                     		Position = 0,
				                                     		Nested = new[]
				                                     		{
				                                     			new PatchRequest
				                                     			{
				                                     				Type =
				                                     					PatchCommandType.Set,
				                                     				Name = "Level",
				                                     				Value = "Gold"
				                                     			},
				                                     		}
				                                     	}
				                                     }, allowStale: false);

				var document = store.DatabaseCommands.Get("patrons/1");

				Assert.Equal("Gold", document.DataAsJson.Value<RavenJArray>("Privilege")[0].Value<string>("Level"));
			}
		}
	}
}