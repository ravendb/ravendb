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

					var foundDocs = session.Query<TestDoc>()
                                           .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                                           .Where(doc => doc.Name.In(nameList)).ToList();

                    Assert.Equal(nameList.Count, foundDocs.Count);
                }
			}
		}

		[Fact]
		public void InListOver256Chars2()
		{
			using (var store = NewRemoteDocumentStore(fiddler: true))
			{
				using (var session = store.OpenSession())
				{
					var nameList = new List<string>();
					var count = 0;
					var index = 0;
					while (count < 0x100)
					{
						var doc = new TestDoc { Name = new string('a', 300) + index };
						session.Store(doc);

						nameList.Add(doc.Name);
						count += (doc.Name.Length + 1);
						index++;
					}
					session.SaveChanges();
					WaitForIndexing(store);

					var foundDocs = session.Query<TestDoc>()
                                           .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                                           .Where(doc => doc.Name.In(nameList)).ToList();

                    Assert.Equal(nameList.Count, foundDocs.Count);
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