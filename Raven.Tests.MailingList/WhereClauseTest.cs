// -----------------------------------------------------------------------
//  <copyright file="WhereClauseTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class WhereClauseTest : RavenTest
	{
		[Fact]
		public void ATest()
		{
			using (var ds = NewDocumentStore())
			{
				using (IDocumentSession session = ds.OpenSession())
				{
					session.Store(new TestEntity(int.MaxValue));
					session.SaveChanges();
				}


				using (IDocumentSession qSession = ds.OpenSession())
				{
					var entities = qSession.Query<TestEntity>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.IntType > 0)
						.ToList();

					Assert.True(entities.Count > 0);
				}
			}
		}

		public class TestEntity
		{
			public TestEntity(int intValue)
			{
				IntType = intValue;
			}

			public string Id { get; set; }
			public int IntType { get; set; }
		}
	}
}