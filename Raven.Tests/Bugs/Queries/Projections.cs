//-----------------------------------------------------------------------
// <copyright file="Projections.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	using Document;

	public class Projections : LocalClientTest
    {
        [Fact]
        public void Can_project_value_from_collection()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Addresses = new[]
                        {
                            new LiveProjection.Address
                            {
                                Name = "Hadera"
                            },
                            new LiveProjection.Address
                            {
                                Name = "Tel Aviv"
                            },
               
                        }
                    });
                    s.SaveChanges();
                }

                var queryResult = store.DatabaseCommands.Query("dynamic",
                                                               new IndexQuery
                                                               {
                                                                   FieldsToFetch = new[] { "Addresses,Name" }
                                                               }, 
                                                               new string[0]);

            	var array = (RavenJArray)queryResult.Results[0]["Addresses"];
            	Assert.Equal(2, array.Length);
				Assert.Equal("Hadera", array[0].Value<string>("Name"));
				Assert.Equal("Tel Aviv", array[1].Value<string>("Name"));
            }
        }


		[Fact]
		public void Can_perform_a_simple_projection_in_a_linq_query()
		{
			using (var store = NewDocumentStore())
			{

				var entity = new Company { Name = "Simple Company", Id = "companies/1" };
				using (var session = store.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var results = session.Query<Company>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "Simple Company")
						.Select(x => new TheCompanyName { Name = x.Name })
						.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("Simple Company", results[0].Name);
				}
			}
		}

        public class User
        {
            public LiveProjection.Address[] Addresses { get; set; }
        }



		public class TheCompanyName
		{
			public string Name { get; set; }
		}
    }
}