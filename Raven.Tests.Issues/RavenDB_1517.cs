// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1517.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1517 : RavenTest
	{
		[Fact]
		public void ShouldNotThrowTooLongUri_LazyIn_FindDuplicates()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new SampleDataIndex().Execute(store);
				using (var session = store.OpenSession())
				{
					//Add fixed Emails to DB
					var email1 = new SubScriber() { Email = "test@gmail.com" };
					var email2 = new SubScriber() { Email = "test2@gmail.com" };
					var email3 = new SubScriber() { Email = "test3@gmail.com" };
					session.Store(email1);
					session.Store(email2);
					session.Store(email3);
					session.SaveChanges();
				}
				//create Imported Emails List
				var importedEmails = new List<SubScriber>();
				//create 2000 Random Imported Emails
				for (int i = 0; i < 2000; i++)
				{
					var randomEmail = new SubScriber() { Email = Guid.NewGuid().ToString() + "@gmail.com" };
					importedEmails.Add(randomEmail);
				}
				using (var session = store.OpenSession())
				{
					var emailsInDb =
						session.Query<SubScriber, SampleDataIndex>()
							.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
							.Lazily();

					Assert.Equal(3, emailsInDb.Value.ToList().Count);

					// Find Duplicates
					var duplicates =
					  session.Query<SubScriber, SampleDataIndex>()
						  .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
						  .Where(c => c.Email.In(importedEmails.Select(cc => cc.Email)))
						  .Take(1024)
						  .Lazily();

					Assert.Equal(2000, importedEmails.Count);
					Assert.Equal(0, duplicates.Value.ToList().Count);
				}
			}
		}

		public class SubScriber
		{
			public string Id { get; set; }
			public string Email { get; set; }
		}
		public class SampleDataIndex : AbstractIndexCreationTask<SubScriber>
		{
			public SampleDataIndex()
			{
				Map = docs => from doc in docs
							  select new
							  {
								  doc.Email
							  };
			}
		}

		private class Student
		{
			public string Email { get; set; }
		}

		private class Students_ByEmailDomain : AbstractIndexCreationTask<Student>
		{
			public Students_ByEmailDomain()
			{
				Map = students => from student in students
								  select new
								  {
									  EmailDomain = student.Email.Split('@').Last(),
								  };
			}
		}

		private string PrepareTooLongUriTest(DocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				session.Store(new Student { Email = "support@hibernatingrhinos.com" });
				session.Store(new Student { Email = "office@company.com" });
				session.SaveChanges();
			}

			new Students_ByEmailDomain().Execute(store);

			var list = new List<string>() { "hibernatingrhinos.com" };
			for (var i = 0; i < 1023; i++) // in Lucene maxClauseCount is set to 1024
				list.Add(Guid.NewGuid().ToString() + Guid.NewGuid().ToString());

			var queryString = String.Join(" OR ", list.Select(i => "EmailDomain: " + i));

			return queryString;
		}

		[Fact]
		public void ShouldNotThrowTooLongUri_LuceneQuery()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var queryString = PrepareTooLongUriTest(store);

				using (var session = store.OpenSession())
				{
                    var query = session.Advanced.DocumentQuery<Student, Students_ByEmailDomain>()
								   .WaitForNonStaleResults()
								   .Where(queryString);

					var value = query.ToList().Count;
					Assert.Equal(1, value);
				}
			}
		}

		[Fact]
		public void ShouldNotThrowTooLongUri_LazyQuery()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var queryString = PrepareTooLongUriTest(store);

				using (var session = store.OpenSession())
				{
                    var query = session.Advanced.DocumentQuery<Student, Students_ByEmailDomain>().WaitForNonStaleResults().Where(queryString);

					var value = query.Lazily().Value;
					Assert.Equal(1, value.Count());
				}
			}
		}

		[Fact]
		public void ShouldNotThrowTooLongUri_DynamicQuery()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Student { Email = "support@hibernatingrhinos.com" });
					session.SaveChanges();
				}

				var list = new List<string>() { "support@hibernatingrhinos.com" };
				for (var i = 0; i < 1023; i++) // in Lucene maxClauseCount is set to 1024
					list.Add(Guid.NewGuid().ToString() + Guid.NewGuid().ToString());

				var queryString = String.Join(" OR ", list.Select(i => "Email: " + i));

				WaitForIndexing(store);

				var query = store.DatabaseCommands.Query("dynamic", new IndexQuery
				{
					Query = queryString
				}, new string[0]);

				Assert.Equal(1, query.Results.Count);
			}
		}

		[Fact]
		public void ShouldNotThrowTooLongUri_StreamQuery()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var queryString = PrepareTooLongUriTest(store);

				WaitForIndexing(store);

				QueryHeaderInformation queryHeaders;
				var enumerator = store.DatabaseCommands.StreamQuery(new Students_ByEmailDomain().IndexName, new IndexQuery
				{
					Query = queryString
				}, out queryHeaders);

				Assert.Equal(1, queryHeaders.TotalResults);

				int count = 0;
				while (enumerator.MoveNext())
				{
					count++;
				}

				Assert.Equal(1, count);
			}
		}
	}
}