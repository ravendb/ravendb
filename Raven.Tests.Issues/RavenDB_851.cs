using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_851 : RavenTest
	{
		[Fact]
		public void ShouldNotAllowComputationInCount()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var ae = Assert.Throws<ArgumentException>(() =>
					                                          session.Query<Foo>()
						                                          .Where(r => r.Items.Count(f => f == "foo") > 1)
						                                          .ToList());
					Assert.Equal("Could not understand expression: .Where(r => (r.Items.Count(f => (f == \"foo\")) > 1))", ae.Message);
					Assert.Equal("Not supported computation: r.Items.Count(f => (f == \"foo\")). You cannot use computation in RavenDB queries (only simple member expressions are allowed).",
						ae.InnerException.Message);
				}
			}
		}

		public class Foo
		{
			public List<string> Items { get; set; }
		}
	}
}