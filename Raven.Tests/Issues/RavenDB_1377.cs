// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1377.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1377 : RavenTest
	{
		/*
		 * Raven should be immune to calendar system changes of diffferent cultures.
		 * We should only store, index, and query using the ISO8601 (Gregorian) calendar.
		 */

		[Fact]
		public void Can_Query_DateTimeOffset_en_US()
		{
			DoDateTimeOffsetTest("en-US");
		}

		[Fact]
		public void Can_Query_DateTimeOffset_ar_SA()
		{
			DoDateTimeOffsetTest("ar-SA");
		}

		[Fact]
		public void Can_Query_DateTime_en_US()
		{
			DoDateTimeTest("en-US");
		}

		[Fact]
		public void Can_Query_DateTime_ar_SA()
		{
			DoDateTimeTest("ar-SA");
		}

		private void DoDateTimeOffsetTest(string culture)
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture(culture);

			var now = DateTimeOffset.UtcNow;

			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { CreatedOn = now });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var q = session.Query<Foo>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.CreatedOn <= now);

					Debug.WriteLine(q);

					var results = q.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(now, results[0].CreatedOn);
					Assert.Equal("CreatedOn:[* TO " + now.UtcDateTime.ToString("o") + "]", q.ToString());
				}
			}
		}

		private void DoDateTimeTest(string culture)
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture(culture);

			var now = DateTime.UtcNow;

			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Bar { CreatedOn = now });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var q = session.Query<Bar>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.CreatedOn <= now);

					Debug.WriteLine(q);

					var results = q.ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal(now, results[0].CreatedOn);
					Assert.Equal("CreatedOn:[* TO " + now.ToString("o") + "]", q.ToString());
				}
			}
		}

		public class Foo
		{
			public DateTimeOffset CreatedOn { get; set; }
		}

		public class Bar
		{
			public DateTime CreatedOn { get; set; }
		}
	}
}
