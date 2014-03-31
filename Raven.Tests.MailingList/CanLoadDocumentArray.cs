using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;

namespace Raven.Tests.MailingList
{
	public class CanLoadDocumentArray : RavenTestBase
	{
		private class Student
		{
			public string Id { get; set; }
			public string Email { get; set; }
			public IEnumerable<string> Friends { get; set; }
		}

		private class Students_WithFriends
			: AbstractIndexCreationTask<Student, Students_WithFriends.Mapping>
		{
			public class Mapping
			{
				public string EmailDomain { get; set; }
				public IEnumerable<string> Friends { get; set; }
			}

			public Students_WithFriends()
			{
				Map = students => from student in students
								  let friends = LoadDocument<Student>(student.Friends)
								  select new Mapping
								  {
									  EmailDomain = student.Email.Split('@').Last(),
									  Friends = friends.Select(a => a.Email)
								  };

				Analyzers.Add(x => x.Friends, "Lucene.Net.Analysis.SimpleAnalyzer, Lucene.Net");
				Indexes.Add(x => x.Friends, FieldIndexing.Analyzed);
			}
		}

		[Fact]
		public void WillSupportLoadDocumentArray()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var student1 = new Student { Email = "support@something.com" };
					var student2 = new Student { Email = "ayende@something.com" };
					var student3 = new Student { Email = "oren@something.com" };

					session.Store(student1);
					session.Store(student2);
					student3.Friends = new List<string>() { student1.Id, student2.Id };
					session.Store(student3);

					session.SaveChanges();
				}

				new Students_WithFriends().Execute(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<Student, Students_WithFriends>()
										 .Customize(customization => customization.WaitForNonStaleResults())
										 .ToList();

					Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);
					Assert.Equal(3, results.Count);
				}
			}
		}
	}
}