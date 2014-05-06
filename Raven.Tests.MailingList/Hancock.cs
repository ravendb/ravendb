// -----------------------------------------------------------------------
//  <copyright file="Hancock.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Hancock : RavenTest
	{
		public enum SomeEnum
		{
			One,
			Two,
			Three	
		}

		public class Foo
		{
			public List<SomeEnum> Items { get; set; }
		}

		[Fact]
		public void CanDetectChanges()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Foo
					{
						Items = new List<SomeEnum>
						{
							SomeEnum.One
						}
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Load<Foo>(1).Items.Add(SomeEnum.Three);
					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					Assert.Equal(2, session.Load<Foo>(1).Items.Count);
				}
			}
		}

	}
}