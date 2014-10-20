// -----------------------------------------------------------------------
//  <copyright file="IndexationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;

using Raven.Client.Indexes;
using Raven.Tests.Bundles.Replication;

using Xunit;

namespace Raven.Tests.Issues
{
	public class IndexationTests : ReplicationBase
	{
		private class Person
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		private class Item
		{
			public string Id { get; set; }
			public byte[] Content { get; set; }
			public string Name { get; set; }
		}

		private class Result
		{
			public string Tag { get; set; }
		}

		[Fact]
		public void ShouldWork()
		{
			var one = CreateStore(requestedStorage: "esent");
			var two = CreateStore(requestedStorage: "esent");

			new RavenDocumentsByEntityName().Execute(two);

			var random = new Random();
			var content = new byte[10000];
			random.NextBytes(content);

			string id = null;

			using (var s1 = one.OpenSession())
			{
				for (int i = 0; i < 10000; i++)
				{
					var item = new Item { Name = "ayende", Content = content };
					s1.Store(item);

					id = item.Id;
				}

				s1.SaveChanges();
			}

			// master / master
			TellFirstInstanceToReplicateToSecondInstance();

			Thread.Sleep(2000);

			for (int i = 0; i < 10; i++)
			{
				using (var s2 = two.OpenSession())
				{
					var item = new Person { Name = "ayende" };
					s2.Store(item);

					s2.SaveChanges();

					Thread.Sleep(500);
				}
			}

			WaitForReplication(two, id);

			WaitForIndexing(two);

			using (var s2 = two.OpenSession())
			{
				var count = s2
					.Query<Result>("Raven/DocumentsByEntityName")
					.Customize(x => x.WaitForNonStaleResults())
					.Count(x => x.Tag == "Items");

				Assert.Equal(10000, count);
			}
		}
	}
}