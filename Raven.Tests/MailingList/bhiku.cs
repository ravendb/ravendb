// -----------------------------------------------------------------------
//  <copyright file="bhiku.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.MailingList
{
	public class Bhiku : RavenTest
	{
		private readonly IDocumentStore store;

		public Bhiku()
		{
			store = NewDocumentStore();
			using (var session = store.OpenSession())
			{
				session.Store(new Student { FirstName = "David", LastName = "Globe" });
				session.Store(new Student { FirstName = "Tyson", LastName = "David" });
				session.Store(new Student { FirstName = "David", LastName = "Jason" });
				session.SaveChanges();
			}

			new Student_ByName().Execute(store);
		}

		[Fact]
		public void CanUseBoost_StartsWith()
		{
			using (var session = store.OpenSession())
			{
				var students = session.Advanced.LuceneQuery<Student>()
					.WaitForNonStaleResults()
					.WhereStartsWith("FirstName", "David").Boost(3)
					.WhereStartsWith("LastName", "David")
					.ToList();

				Assert.Equal(3, students.Count);
				Assert.Equal("students/2", students[2].Id);

				try
				{
						.ToList();

					Assert.Equal(3, students.Count);

						.OrderBy(Constants.TemporaryScoreValue, "LastName")
						.ToList();

					Assert.Equal(3, students.Count);

					Assert.Equal("students/1", students[0].Id);
					Assert.Equal("students/3", students[1].Id);
				}
				catch (EqualException)
				{
					Assert.Equal("students/3", students[0].Id);
					Assert.Equal("students/1", students[1].Id);
				}
			}
		}

		[Fact]
		public void CanUseBoost_Equal()
		{
			using (var session = store.OpenSession())
			{
				var queryable = session.Query<Student, Student_ByName>()
					.Customize(x => x.WaitForNonStaleResults())
					.Where(x => x.FirstName == ("David") || x.LastName == ("David"));
				var students = queryable
					.ToList();

				Assert.Equal(3, students.Count);
				Assert.Equal("students/2", students[2].Id);

				try
				{
					Assert.Equal("students/1", students[0].Id);
					Assert.Equal("students/3", students[1].Id);
				}
				catch (EqualException)
				{
					Assert.Equal("students/3", students[0].Id);
					Assert.Equal("students/1", students[1].Id);
				}
			}

		}

		private class Student
		{
			public string Id { get; set; }
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public DateTime DateOfBirth { get; set; }
		}

		private class Student_ByName : AbstractIndexCreationTask<Student>
		{
			public Student_ByName()
			{
				Map = students => from s in students
								  select new
								  {
									  FirstName = s.FirstName.Boost(6),
									  s.LastName,
									  s.DateOfBirth,
								  };
			}
		}
	}
}
