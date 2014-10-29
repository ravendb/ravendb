// -----------------------------------------------------------------------
//  <copyright file="NimaHa.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class NimaHa : RavenTest
	{
		[Fact]
		public void NullValueTest()
		{
			using (var store = NewDocumentStore())
			{
				const string house1 = @"{
                    Rent : 1200
                }";

				const string house2 = @"{
                                Rent : null
                                }";

				const string house3 = @"{
                                }";

				const string metadata = @"{ 
                                    ""Raven-Entity-Name"" : ""Houses"" 
                                 }";


				store.DatabaseCommands.PutIndex("HouseByRent", new IndexDefinition
				{
					Map = "from doc in docs.Houses select new { Rent=doc.Inner.ContainsKey(\"Rent\")?doc.Rent:null}",

					Name = "HouseByRent"

				});



				store.DatabaseCommands.Put("house/1", Etag.Empty, RavenJObject.Parse(house1),
					RavenJObject.Parse(metadata));
				store.DatabaseCommands.Put("house/2", Etag.Empty, RavenJObject.Parse(house2),
					RavenJObject.Parse(metadata));
				store.DatabaseCommands.Put("house/3", Etag.Empty, RavenJObject.Parse(house3),
					RavenJObject.Parse(metadata));

				//Wait for non stale results
				using (var session = store.OpenSession())
				{
					var list = session.Query<dynamic>("HouseByRent").Customize(x => x.WaitForNonStaleResults()).ToList();
					Assert.Equal(3, list.Count);
				}

				var query = store.DatabaseCommands.Query("HouseByRent",
					new IndexQuery { Query = "*:* AND -Rent:[[NULL_VALUE]]" },
					null);

				Assert.Equal(1, query.TotalResults);


			}
		} 
	}
}