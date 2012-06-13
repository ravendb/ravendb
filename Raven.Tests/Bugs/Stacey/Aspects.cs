using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Linq.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace Raven.Tests.Bugs.Stacey
{
	public class Aspects : InServerDatabase
	{
		private IDocumentStore store;

		public Aspects()
		{
			store = DocumentStore();
		}

		[Fact]
		public void Aspects_Can_Be_Installed()
		{
			// currency
			var currency = new Currency[]
			               	{
			               		new Currency
			               			{
			               				Name = "Money"
			               			},
			               		new Currency
			               			{
			               				Name = "Points"
			               			}
			               	};

			// assert
			Assert.DoesNotThrow(() =>
			                    	{
			                    		using (var session = store.OpenSession())
			                    		{
			                    			// ensure optimistic concurrency
			                    			session.Advanced.UseOptimisticConcurrency = true;

			                    			// try to insert the apertures into the database
			                    			session.Store(currency[0]);
			                    			session.Store(currency[1]);

			                    			session.SaveChanges();
			                    		}
			                    	});

			// arrange
			var aspects = new Aspect[]
			              	{
			              		new Aspect
			              			{
			              				Name = "Strength",
			              				Kind = Kind.Attribute,
			              				Category = Category.Physical
			              			},
			              		new Aspect
			              			{
			              				Name = "Stamina",
			              				Kind = Kind.Attribute,
			              				Category = Category.Physical
			              			}
			              	};


			// assert
			Assert.DoesNotThrow(() =>
			                    	{
			                    		using (var session = store.OpenSession())
			                    		{
			                    			// ensure optimistic concurrency
			                    			session.Advanced.UseOptimisticConcurrency = true;

			                    			// try to insert the aspects into the database
			                    			session.Store(aspects[0]);
			                    			session.Store(aspects[1]);

			                    			session.SaveChanges();

			                    			// try to query each of the newly inserted aspects.
			                    			var query = session.Query<Aspect>().ToList();

			                    			// unit test each aspect.
			                    			foreach (var aspect in query)
			                    			{
			                    				Assert.NotNull(aspect.Id);
			                    				Assert.NotNull(aspect.Name);
			                    				Assert.NotNull(aspect.Kind);
			                    				Assert.NotNull(aspect.Category);
			                    			}
			                    		}
			                    	});

			// update
			Assert.DoesNotThrow(() =>
			                    	{
			                    		using (var session = store.OpenSession())
			                    		{
			                    			// ensure optimistic concurrency
			                    			session.Advanced.UseOptimisticConcurrency = true;

			                    			// create an array to hold the results.
			                    			var results = new Aspect[2];

			                    			// try to query each of the newly inserted aspects.
			                    			results[0] = session.Load<Aspect>("aspects/1");
			                    			results[1] = session.Load<Aspect>("aspects/2");

			                    			// load the flexskill points currency.
			                    			var points = session.Load<Currency>("currencies/2");

			                    			results[0].Path = new Path
			                    			                  	{
			                    			                  		Steps = new List<Step>
			                    			                  		        	{
			                    			                  		        		new Step
			                    			                  		        			{
			                    			                  		        				Cost = 5,
			                    			                  		        				Number = 1,
			                    			                  		        				Currency = points.Id,
			                    			                  		        				Requirements = new List<Requirement>
			                    			                  		        				               	{
			                    			                  		        				               		new Requirement
			                    			                  		        				               			{
			                    			                  		        				               				What = results[1].Id,
			                    			                  		        				               				Value = 2
			                    			                  		        				               			},
			                    			                  		        				               		new Requirement
			                    			                  		        				               			{
			                    			                  		        				               				What = points.Id,
			                    			                  		        				               				Value = 5
			                    			                  		        				               			}
			                    			                  		        				               	}
			                    			                  		        			}
			                    			                  		        	}
			                    			                  	};

			                    			session.SaveChanges();
			                    		}
			                    	});

			// assert
			Assert.DoesNotThrow(() =>
			                    	{
			                    		using (var session = store.OpenSession())
			                    		{
			                    			// ensure optimistic concurrency
			                    			session.Advanced.UseOptimisticConcurrency = true;

			                    			// create an array to hold the results.
			                    			// try to query each of the newly inserted aspects.
			                    			var results = session.Include("Path.Steps,Requirements,What").Load<Aspect>("aspects/1");

			                    			// the first requirement should be an aspect
			                    			var requirements = new Entity[2];

			                    			requirements[0] = session.Load<Aspect>(results.Path.Steps[0].Requirements[0].What);
			                    			requirements[1] = session.Load<Currency>(results.Path.Steps[0].Requirements[1].What);

			                    			Assert.IsType<Aspect>(requirements[0]);
			                    			Assert.IsType<Currency>(requirements[1]);

			                    			Assert.Equal(1, session.Advanced.NumberOfRequests);

			                    			session.SaveChanges();

			                    			Console.WriteLine(JsonConvert.SerializeObject(results.Path.Steps[0].Requirements[0]));
			                    			Console.WriteLine(JsonConvert.SerializeObject(results.Path.Steps[0].Requirements[1]));
			                    		}
			                    	});
		}

		[Fact]
		public void Index_can_be_queried()
		{
			store.DatabaseCommands.PutIndex("AspectsByName", new IndexDefinitionBuilder<Aspect>
			                                                 	{
			                                                 		Map = orders => from order in orders
			                                                 		                select new {order.Name}
			                                                 	});
			store.DatabaseCommands.PutIndex("test", new IndexDefinitionBuilder<Entity>()
			                                        	{
			                                        		Map = docs => from i in docs.WhereEntityIs<Entity>("Aspects", "Currencies")
			                                        		              select new {i.Name}
			                                        	}.ToIndexDefinition(store.Conventions));

			// arrange
			var aspects = new Aspect[2]
			              	{
			              		new Aspect
			              			{
			              				Name = "Strength",
			              				Kind = Kind.Attribute,
			              				Category = Category.Physical
			              			},
			              		new Aspect
			              			{
			              				Name = "Stamina",
			              				Kind = Kind.Attribute,
			              				Category = Category.Physical
			              			}
			              	};


			// assert
			Assert.DoesNotThrow(() =>
			                    	{
			                    		using (var session = store.OpenSession())
			                    		{
			                    			// ensure optimistic concurrency
			                    			session.Advanced.UseOptimisticConcurrency = true;

			                    			// try to insert the aspects into the database
			                    			session.Store(aspects[0]);
			                    			session.Store(aspects[1]);

			                    			session.SaveChanges();

			                    			// try to query each of the newly inserted aspects.
			                    			var query = session.Query<Aspect>().ToList();

			                    			// unit test each aspect.
			                    			foreach (var aspect in query)
			                    			{
			                    				Assert.NotNull(aspect.Id);
			                    				Assert.NotNull(aspect.Name);
			                    				Assert.NotNull(aspect.Kind);
			                    				Assert.NotNull(aspect.Category);
			                    			}
			                    		}
			                    	});

			// assert
			Assert.DoesNotThrow(() =>
			                    	{
			                    		using (var session = store.OpenSession())
			                    		{
			                    			// ensure optimistic concurrency
			                    			session.Advanced.UseOptimisticConcurrency = true;

			                    			// create an array to hold the results.
			                    			// try to query each of the newly inserted aspects.

			                    			var results = session.Query<Aspect>("AspectsByName")
			                    				.Customize(n => n.WaitForNonStaleResults())
			                    				.Where(n => n.Name == "Strength")
			                    				.ToList();

			                    			Console.WriteLine(JsonConvert.SerializeObject(results));
			                    		}
			                    	});
		}
	}
}