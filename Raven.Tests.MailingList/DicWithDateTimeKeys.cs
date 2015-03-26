// -----------------------------------------------------------------------
//  <copyright file="DicWithDateTimeKeys.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class DicWithDateTimeKeys : RavenTest
	{
		public class A
		{
			public IDictionary<DateTimeOffset, string> Items { get; set; }
		}


		[Fact]
		public void CanSaveAndLoad()
		{
			using (var store = NewDocumentStore())
			{
				var dateTimeOffset = DateTimeOffset.Now;
				using (var session = store.OpenSession())
				{
					session.Store(new A
					{
						Items = new Dictionary<DateTimeOffset, string>
						{
							{dateTimeOffset, "a"}
						}
					});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var load = session.Load<A>(1);
					Assert.Equal("a", load.Items[dateTimeOffset]);
				}
			}
		}
	}
}