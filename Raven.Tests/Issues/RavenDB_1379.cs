using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1379 : RavenTest
	{
		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public void GetDocumentsWithIdStartingWith_Should_Not_Count_Excluded_Docs(string requestedStorage)
		{
			using(var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Put("FooBar1", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo2", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar3", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar11", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar12", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar21", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar5", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo7", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar111", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("BarFoo6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar6", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBar8", null, new RavenJObject(), new RavenJObject(), null);

				var fetchedDocuments = documentStore.DocumentDatabase.GetDocumentsWithIdStartingWith("FooBar", String.Empty, "1*", 0, 4);

				var documentsList = fetchedDocuments.ToList();
				var foundDocKeys = documentsList.Select(doc => doc.Value<RavenJObject>("@metadata"))
												.Select(doc => doc.Value<string>("@id"))
												.ToList();

				Assert.Equal(4,foundDocKeys.Count);
				Assert.Contains("FooBar3", foundDocKeys);
				Assert.Contains("FooBar21", foundDocKeys);
				Assert.Contains("FooBar5", foundDocKeys);
				Assert.Contains("FooBar6", foundDocKeys);
			}
		}

		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public void GetDocumentsWithIdStartingWith_Should_Not_Count_Excluded_Docs_WithNonZeroStart(string requestedStorage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: requestedStorage))
			{
				documentStore.DocumentDatabase.Put("FooBarAA", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarBB", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarCC", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarDD", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarDA", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarEE", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarFF", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarGG", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarHH", null, new RavenJObject(), new RavenJObject(), null);
				documentStore.DocumentDatabase.Put("FooBarKK", null, new RavenJObject(), new RavenJObject(), null);

				var fetchedDocuments = documentStore.DocumentDatabase.GetDocumentsWithIdStartingWith("FooBar", String.Empty, "*A", 2, 4);

				var documentsList = fetchedDocuments.ToList();
				var foundDocKeys = documentsList.Select(doc => doc.Value<RavenJObject>("@metadata"))
												.Select(doc => doc.Value<string>("@id"))
												.ToList();

				Assert.Equal(4, foundDocKeys.Count);
				Assert.Contains("FooBarDD", foundDocKeys);
				Assert.Contains("FooBarEE", foundDocKeys);
				Assert.Contains("FooBarFF", foundDocKeys);
				Assert.Contains("FooBarGG", foundDocKeys);
			}
		}
	
	}
}
