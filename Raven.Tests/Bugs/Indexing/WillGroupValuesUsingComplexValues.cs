//-----------------------------------------------------------------------
// <copyright file="WillGroupValuesUsingComplexValues.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Abstractions.Data;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class WillGroupValuesUsingComplexValues : RavenTest
	{
		[Fact]
		public void CanGroupByComplexObject()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "Oren", Address = new { City = "New York", Street = "Braodway" } });
					session.Store(new { Name = "Eini", Address = new { City = "Halom", Street = "Silk" } });
					session.Store(new { Name = "Rahien", Address = new { City = "Halom", Street = "Silk" } });
					session.Store(new { Name = "Ayende", Address = new { City = "New York", Street = "Braodway" } });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var objects = session.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count, "Address")
						.OrderBy("-Address")
						.WaitForNonStaleResults(TimeSpan.FromMinutes(1))
						.ToArray();

					Assert.Equal(2, objects.Length);

					Assert.Equal("2", objects[0].Count);
					Assert.Equal("New York", objects[0].Address.City);


					Assert.Equal("2", objects[1].Count);
					Assert.Equal("Halom", objects[1].Address.City);
				}
			}
		}
	}
}