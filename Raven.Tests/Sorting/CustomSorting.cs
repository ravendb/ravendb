// -----------------------------------------------------------------------
//  <copyright file="CustomSorting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Parser;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Rhino.Mocks.Constraints;
using Xunit;

namespace Raven.Tests.Sorting
{
	public class CustomSorting : RavenTest
	{
		public class User
		{
			public string Name;
		}

		public class User_Search : AbstractIndexCreationTask<User>
		{
			public User_Search()
			{
				Map = users =>
					from user in users
					select new { user.Name };

				Store(x => x.Name, FieldStorage.Yes);
			}
		}

	    public class Customer
	    {
	        public class Tag
	        {
                public int Id { get; set; }
                public string Descrtiption { get; set; }
	        }
            public string Name { get; set; }
            public List<Tag> PayingTags { get; set; }
	        public int TagId { get; set; }
	        public int Points { get; set; }
	    }

	    public class Customer_Search : AbstractIndexCreationTask<Customer>
	    {
	        public Customer_Search()
	        {
	            Map = customers =>
	                from customer in customers
	                select new
	                {
                        customer.TagId,
                        customer.Points,                       
                        _ = from pTag in customer.PayingTags
                                  select new[]
                            {
                                CreateField("PayingTag_" + pTag.Id , true)
                            }
	                };
                Store(Constants.AllFields, FieldStorage.Yes);
	        }
	    }

		[Fact]
		public void AscendingPrefix()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "Maxim" });
					session.Store(new User { Name = "Oren" });
                    session.Store(new User { Name = "Grisha" });
                    session.Store(new User { Name = "Michael" });
					session.SaveChanges();
				}

				new User_Search().Execute(store);
				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
                    var users = session.Query<User, User_Search>()
						.Customize(x => x.CustomSortUsing(typeof (SortByDynamicFields).AssemblyQualifiedName))
                        .AddTransformerParameter("customTagId", 1)
                        .AddTransformerParameter("cityId", 2)
						.ToList();

                    Assert.Equal(users[0].Name, "Michael");
                    Assert.Equal(users[1].Name, "Oren");
                    Assert.Equal(users[2].Name, "Grisha");
                    Assert.Equal(users[3].Name, "Maxim");
				}

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User, User_Search>()
                        .Customize(x => x.CustomSortUsing(typeof(SortByNumberOfCharactersFromEnd).AssemblyQualifiedName))
                        .AddTransformerParameter("len", 1)
                        .ToList();

                    Assert.Equal(users[0].Name, "Grisha");
                    Assert.Equal(users[1].Name, "Michael");
                    Assert.Equal(users[2].Name, "Maxim");
                    Assert.Equal(users[3].Name, "Oren");
                    
                }
			}
		}

        [Fact]
        public void DescendingPrefix()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Maxim" });
                    session.Store(new User { Name = "Oren" });
                    session.Store(new User { Name = "Grisha" });
                    session.Store(new User { Name = "Michael" });
                    session.SaveChanges();
                }

                new User_Search().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User, User_Search>()
                        .Customize(x => x.CustomSortUsing(typeof(SortByNumberOfCharactersFromEnd).AssemblyQualifiedName,true))
                        .AddTransformerParameter("len", 3)
                        .ToList();

                    
                    
                    
                    Assert.Equal(users[0].Name, "Maxim");
                    Assert.Equal(users[1].Name, "Grisha");
                    Assert.Equal(users[2].Name, "Oren");
                    Assert.Equal(users[3].Name, "Michael");
                    
                    
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User, User_Search>()
                        .Customize(x => x.CustomSortUsing(typeof(SortByNumberOfCharactersFromEnd).AssemblyQualifiedName,true))
                        .AddTransformerParameter("len", 1)
                        .ToList();

                    Assert.Equal(users[0].Name, "Oren");
                    Assert.Equal(users[1].Name, "Maxim");
                    Assert.Equal(users[2].Name, "Michael");
                    Assert.Equal(users[3].Name, "Grisha");
                }
            }
        }

        [Fact]
        public void CustomSortByDynamicFields()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer
                    {
                        Name = "Johnny",
                        PayingTags = new List<Customer.Tag>()
                        {
                            new Customer.Tag()
                            {
                                Id = 1,
                                Descrtiption = "Loud Book Reading"
                            },
                        },
                        Points = 5,
                        TagId = 2
                    }, "Customers/Johnny");

                    session.Store(new Customer
                    {
                        Name = "Abraham",
                        PayingTags = new List<Customer.Tag>()
                        {
                            new Customer.Tag()
                            {
                                Id = 1,
                                Descrtiption = "Loud Book Reading"
                            },
                            new Customer.Tag()
                            {
                                Id = 2,
                                Descrtiption = "Menicure"
                            }
                        },
                        Points = 3,
                        TagId = 1
                    }, "Customers/Abraham");

                    session.Store(new Customer
                    {
                        Name = "Richard",
                        PayingTags = new List<Customer.Tag>()
                        {
                            new Customer.Tag()
                            {
                                Id = 1,
                                Descrtiption = "Loud Book Reading"
                            },
                            
                        },
                        Points = 3,
                        TagId = 1
                    }, "Customers/Richard");

                    session.Store(new Customer
                    {
                        Name = "Vasily",
                        PayingTags = new List<Customer.Tag>()
                        {
                            new Customer.Tag()
                            {
                                Id = 1,
                                Descrtiption = "Loud Book Reading"
                            },
                            
                        },
                        Points = 4,
                        TagId = 1
                    }, "Customers/Vasily");

                    session.Store(new Customer
                    {
                        Name = "Josef",
                        PayingTags = new List<Customer.Tag>()
                        {
                            new Customer.Tag()
                            {
                                Id = 1,
                                Descrtiption = "Loud Book Reading"
                            },
                            new Customer.Tag()
                            {
                                Id = 2,
                                Descrtiption = "Menicure"
                            }
                        },
                        Points = 7,
                        TagId = 2
                    }, "Customers/Josef");
                    session.SaveChanges();
                }
                new Customer_Search().Execute(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<Customer, Customer_Search>()
                       .CustomSortUsing(typeof(SortByDynamicFields).AssemblyQualifiedName, true)
                        .Where("PayingTag_1:true OR TagId:2")
                       .SetTransformerParameters(new Dictionary<string, RavenJToken>()
                    {
                        {"customTagId", 2}
                    }).ToList();

                    Assert.Equal(results[0].Name, "Josef");
                    Assert.Equal(results[1].Name, "Abraham");
                    Assert.Equal(results[2].Name, "Johnny");
                    Assert.Equal(results[3].Name, "Vasily");
                    Assert.Equal(results[4].Name, "Richard");
                }
            }
        }
        
	}
}