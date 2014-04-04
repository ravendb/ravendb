using System;
using System.Linq;
using System.Collections.Generic;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class LinqInExtensionTests : RavenTest
	{
		[Fact]
		public void InListOver256Chars()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var nameList = new List<string>();
					var count = 0;

					while (count < 0x100)
					{
						var doc = new TestDoc { Name = Convert.ToBase64String(Guid.NewGuid().ToByteArray()) };
						session.Store(doc);

						nameList.Add(doc.Name);
						count += (doc.Name.Length + 1);
					}

					session.SaveChanges();

					var foundDocs = session.Query<TestDoc>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Where(doc => doc.Name.In(nameList)).ToList();

					WaitForUserToContinueTheTest(store);

					session.Query<TestDoc>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Where(doc => doc.Name.In(nameList)).ToList();
					Assert.True(foundDocs.Count == nameList.Count);


				}
			}
		}

		public class TestDoc
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

	}
}