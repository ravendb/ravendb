using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class GuidProjection : RavenTest
	{
		public class TestView
		{
			public Guid TestField { get; set; }
		}

		[Fact]
		public void CanProjectGuids()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestView {TestField = Guid.NewGuid()});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var good = session.Query<TestView>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Select(x => new {x.TestField}).ToArray();
					var error = session.Query<TestView>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Select(x => x.TestField).ToArray();
					var error2 = session.Query<TestView>().Select(x => (Guid)x.TestField).ToArray();
				}
			}
		}
	}
}
