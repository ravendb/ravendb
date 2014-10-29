// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1540.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1540 : RavenTest
	{
		[Fact]
		public void CanUseContainWithEnums_OnIEnumerable()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var document1 = new Document { Name = "Test", Events = new[] { Event.Event1, Event.Event2 } };
					var document2 = new Document { Name = "Test2" };

					var document3 = new Document { Name = "Test", Events = new[] { Event.Event2 } };

					session.Store(document1);
					session.Store(document2);

					session.Store(document3);

					session.SaveChanges();
				}


				new NormalMapping().Execute(store);

				using (var session = store.OpenSession())
				{

					var results = session.Query<NormalMapping.Mapping, NormalMapping>()
					.Customize(customization => customization.WaitForNonStaleResults())

										 .Where(a => a.Events.Contains(Event.Event2))
										 .OfType<Document>()
					.ToArray();


					Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);
					Assert.Equal(2, results.Count()); // Will result into 3 :(

				}
			}
		}

		[Fact]
		public void CanUseContainWithIntegers_OnList()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var document1 = new Document { Name = "Test", Integers = new List<int> { 1, 2 } };
					var document2 = new Document { Name = "Test2" };

					var document3 = new Document { Name = "Test", Integers = new List<int> { 2 } };

					session.Store(document1);
					session.Store(document2);

					session.Store(document3);

					session.SaveChanges();
				}


				new NormalMapping().Execute(store);

				using (var session = store.OpenSession())
				{

					var results = session.Query<NormalMapping.Mapping, NormalMapping>()
					.Customize(customization => customization.WaitForNonStaleResults())

										 .Where(a => a.Integers.Contains(2))
										 .OfType<Document>()
					.ToArray();


					Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);
					Assert.Equal(2, results.Count()); // Will result into 3 :(

				}
			}
		}

		private enum Event
		{
			Event1,
			Event2,
			Event3
		}

		private class Document
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public IEnumerable<Event> Events { get; set; }
			public List<int> Integers { get; set; }

		}

		private class NormalMapping
					: AbstractIndexCreationTask<Document, NormalMapping.Mapping>
		{
			public class Mapping
			{
				public string Name { get; set; }
				public List<int> Integers { get; set; }
				public IEnumerable<Event> Events { get; set; }
			}

			public NormalMapping()
			{

				Map = results => from result in results
								 select new Mapping
								 {

									 Name = result.Name,
									 Events = result.Events,
									 Integers = result.Integers
								 };

			}
		}
	}
}