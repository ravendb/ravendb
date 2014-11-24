using System;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class UriProperty : RavenTest
	{
		public class Employer
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public Uri Website { get; set; }
			public Uri Logo { get; set; }
			public string About { get; set; }
		}

		[Fact]
		public void Should_not_cause_ravendb_to_think_it_is_always_changed()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Employer
					{
						Website = new Uri("http://hibernatingrhinos.com")
					});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var employer = session.Load<Employer>(1);
					Assert.False(session.Advanced.HasChanged(employer));
				}
			}
		}
	}
}