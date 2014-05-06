using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Raven.Tests.Common;

using Xunit;
using Raven.Client.Document;

namespace Raven.Tests.Bugs.Indexing
{
	public class RemoteIndexingOnDictionary : RavenTest
	{

		[Fact]
		public void CanIndexOnRangeForNestedValuesForDictionaryAsPartOfDictionary()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new UserWithIDictionary
					{
						NestedItems = new Dictionary<string, NestedItem>
					    {
					        { "Color", new NestedItem{ Value=50 } }
					    }
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
                    Assert.DoesNotThrow(() => s.Advanced.DocumentQuery<UserWithIDictionary>()
					                          	.WhereEquals("NestedItems,Key", "Color")
					                          	.AndAlso()
					                          	.WhereGreaterThan("NestedItems,Value.Value", 10)
					                          	.ToArray());
				}
			}
		}

		#region Nested type: UserWithIDictionary / NestedItem
		public class UserWithIDictionary
		{
			public string Id { get; set; }
			public IDictionary<string, string> Items { get; set; }
			public IDictionary<string, NestedItem> NestedItems { get; set; }
		}

		public class NestedItem
		{
			public string Name { get; set; }
			public double Value { get; set; }
		}

		#endregion
	}
}
