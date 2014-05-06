// -----------------------------------------------------------------------
//  <copyright file="WhenQueringAsync.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class WhenQueringAsync : RavenTest
	{
		[Fact]
		public async Task Can_project_to_named_property()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Student { Email = "support@hibernatingrhinos.com" });
					session.SaveChanges();
				}

				new Students_ByEmailDomain().Execute(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<Student, Students_ByEmailDomain>()
										 .Customize(customization => customization.WaitForNonStaleResults())
										 .Select(c => new
										 {
											 EmailProp = c.Email
										 })
										 .ToList();

					Assert.False(results.Any(c => string.IsNullOrEmpty(c.EmailProp)));
				}

				using (var session = store.OpenAsyncSession())
				{
					var results = await session.Query<Student, Students_ByEmailDomain>()
											   .Customize(customization => customization.WaitForNonStaleResults())
											   .Select(c => new
											   {
												   c.Email
											   })
											   .ToListAsync();

					Assert.False(results.Any(c => string.IsNullOrEmpty(c.Email)));
				}

				using (var session = store.OpenAsyncSession())
				{
					var results = await session.Query<Student, Students_ByEmailDomain>()
											   .Customize(customization => customization.WaitForNonStaleResults())
											   .Select(c => new
											   {
												   EmailProp = c.Email
											   })
											   .ToListAsync();

					Assert.Equal(1, results.Count);
					Assert.False(results.Any(c => string.IsNullOrEmpty(c.EmailProp)));
				}
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
									  EmailDomain = student.Email.Split('@').Last()
								  };
			}
		}
	}
}