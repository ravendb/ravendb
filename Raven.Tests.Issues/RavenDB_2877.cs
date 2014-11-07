// -----------------------------------------------------------------------
//  <copyright file="RavenDB2854.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Config;
using Raven.Database.Server.Controllers;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2877 : RavenTest
	{
        
		public class Person
		{
			public string Name { get; set; }
            public List<string> Offices {get;set;}
		}

	    public class Office
	    {
            public string FacilityName { get; set; }
            public int OfficeNumber { get; set; }
	    }

        public class PersonsIndex:AbstractIndexCreationTask<Person> 
	    {
            public class Result
            {
                public string Name { get; set; }
            }
            public PersonsIndex()
            {
                Map = results => from result in results
                    select new Result
                    {
                        Name = result.Name
                    };
            }
	    }
            
            
            [Fact]
		public async Task CanHandleHandleLongUrl()
		{
			using (var store = NewDocumentStore())
			{
                new PersonsIndex().Execute(store);
				store.InitializeProfiling();
			    store.Conventions.MaxLengthOfQueryUsingGetUrl = 32;
                var offices = Enumerable.Range(1, 20).Select(x => new Office() { FacilityName = "Main Offices", OfficeNumber = x });
				Guid id;
				using (var s = store.OpenSession())
				{
				    foreach (var office in offices)
				    {
                        s.Store(office, "office/" + office.OfficeNumber);
				    }

				    var person = new Person()
				    {
				        Name = "John",
                        Offices = offices.Select(x => "office/" + x.OfficeNumber).ToList()
				    };

                    s.Store(person, "person/1");

                    s.SaveChanges();
				}

                using (var s = store.OpenSession())
                {
                    var results = s.Query<Person, PersonsIndex>().Customize(x=>x.WaitForNonStaleResultsAsOfLastWrite());
                    Assert.DoesNotThrow(() => results.ToList());
                    Assert.DoesNotThrow(() => results.FirstOrDefault(x => x.Name == "John"));
                    Assert.DoesNotThrow(() => results.Include<Person>(x => x.Offices).ToList());

                    results = s.Query<Person,PersonsIndex>();
                    Assert.DoesNotThrow(() => s.Advanced.Stream<Person>(results).MoveNext());
                    Assert.DoesNotThrow(() => s.Advanced.Stream<Person>(results.Where(x => x.Name == "John")).MoveNext());
                }

                using (var s = store.OpenAsyncSession())
                {
                    var results = s.Query<Person>();
                    await results.ToListAsync();
                    await results.FirstOrDefaultAsync(x => x.Name == "John");
                    await results.Include<Person>(x => x.Offices).ToListAsync();

                    results = s.Query<Person, PersonsIndex>();
                    await s.Advanced.StreamAsync<Person>(results).ConfigureAwait(false);
                    await s.Advanced.StreamAsync<Person>(results.Where(x => x.Name == "John")).ConfigureAwait(false);
                    
                }

			    
			}
		}
		
		
	}
}