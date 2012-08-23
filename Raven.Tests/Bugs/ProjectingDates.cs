//-----------------------------------------------------------------------
// <copyright file="ProjectingDates.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ProjectingDates : RavenTest
	{
		[Fact]
		public void CanSaveCachedVery()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Regs",
												new IndexDefinitionBuilder<Registration, Registration>
				                                {
				                                	Map = regs => from reg in regs
																  select new { reg.RegisteredAt },
													Stores = { { x => x.RegisteredAt, FieldStorage.Yes } }
				                                });

				using(var session = store.OpenSession())
				{
					session.Store(new Registration
					{
						RegisteredAt = new DateTime(2010, 1, 1),
						Name = "ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var registration = session.Advanced.LuceneQuery<Registration>("Regs")
						.SelectFields<Registration>("RegisteredAt")
						.WaitForNonStaleResults()
						.First();
					Assert.Equal(new DateTime(2010, 1, 1,0,0,0,DateTimeKind.Local), registration.RegisteredAt);
					Assert.NotNull(registration.Id);
					Assert.Null(registration.Name);
				}
			}
		}

		public class Registration
		{
			public string Id { get; set; }
			public DateTime RegisteredAt { get; set; }

		    public string Name { get; set; }
		}
	}
}
