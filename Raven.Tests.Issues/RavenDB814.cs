// -----------------------------------------------------------------------
//  <copyright file="RavenDB814.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Issues
{
	public class Q14235692 : RavenTestBase
	{
		public class Company
		{
			public string Name { get; set; }
			public string Country { get; set; }
		}

		[Fact]
		public void Empty_Strings_Can_Be_Used_In_Where_Equals()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Company { Name = "A", Country = "USA" });
					session.Store(new Company { Name = "B", Country = "" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Company>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(c => c.Country == "")
										 .ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("B", results[0].Name);
				}
			}
		}

		[Fact]
		public void Empty_Strings_Can_Be_Used_In_Where_In_Once()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Company { Name = "A", Country = "USA" });
					session.Store(new Company { Name = "B", Country = "" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Company>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(c => c.Country.In(new[] { "" }))
										 .ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("B", results[0].Name);
				}
			}
		}

		[Fact]
		public void Empty_Strings_Can_Be_Used_In_Where_In_Twice()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Company { Name = "A", Country = "USA" });
					session.Store(new Company { Name = "B", Country = "" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Company>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(c => c.Country.In(new[] { "", "" }))
										 .ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("B", results[0].Name);
				}
			}
		}

		[Fact]
		public void Empty_Strings_Can_Be_Used_In_Where_In_Thrice()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Company { Name = "A", Country = "USA" });
					session.Store(new Company { Name = "B", Country = "" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Company>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(c => c.Country.In(new[] { "", "", "" }))
										 .ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("B", results[0].Name);
				}
			}
		}

		[Fact]
		public void Empty_Strings_Can_Be_Used_In_Where_In_With_Other_Data()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Company { Name = "A", Country = "USA" });
					session.Store(new Company { Name = "B", Country = "" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Company>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(c => c.Country.In(new[] { "USA", "" }))
										 .ToList();

					Assert.Equal(2, results.Count);
				}
			}
		}
	}
}