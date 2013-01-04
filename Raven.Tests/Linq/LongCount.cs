// -----------------------------------------------------------------------
//  <copyright file="LongCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Linq
{
	using Raven.Abstractions;
	using System;
	using System.Linq;
	using Xunit;

	public class LongCount : RavenTest
	{
		private class TestDoc
		{
			public string Name { get; set; }
		}

		[Fact]
		public void CanQueryLongCount()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var doc = new TestDoc { Name = "foo" };
					session.Store(doc);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					long count = session.Query<TestDoc>()
						.Customize(x=>x.WaitForNonStaleResults())
						.LongCount();
					Assert.Equal(1, count);
				}
			}
		}
	}
}