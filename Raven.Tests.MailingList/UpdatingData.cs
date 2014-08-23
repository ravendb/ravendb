// -----------------------------------------------------------------------
//  <copyright file="UpdatingData.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class UpdatingData : RavenTest
	{
		[Fact]
		public void LoadFails()
		{
			using (DocumentStore store = NewRemoteDocumentStore())
			{
				using (IDocumentSession session1 = store.OpenSession())
				{
					// Store Sample Data in remote database (RavenDB 3 - #3436)
					session1.Store(new SampleData {Id = "sample/1", Name = "First", Type = 1});
					session1.SaveChanges();
				}
				using (IDocumentSession session2 = store.OpenSession())
				{
					// Load and modify sample data
					var sampleData = session2.Load<SampleData>("sample/1");
					sampleData.Type = 2;
					session2.SaveChanges();
				}
				using (IDocumentSession session3 = store.OpenSession())
				{
					// Load modified sample data ... fails.
					var sampleData = session3.Load<SampleData>("sample/1");
					Assert.Equal(2, sampleData.Type); // Failure Expected: 2    Actual: 1
				}
			}
		}

		public class SampleData
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public int Type { get; set; }
		}
	}
}