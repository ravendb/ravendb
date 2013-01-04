//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class IndexingOnDictionary : RavenTest
	{
		[Fact]
		public void CanIndexValuesForDictionary()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User
					        	{
					        		Items = new Dictionary<string, string>
					        		        	{
					        		        		{"Color", "Red"}
					        		        	}
					        	});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Advanced.LuceneQuery<User>()
						.WhereEquals("Items.Color", "Red")
						.ToArray();
					Assert.NotEmpty(users);
				}
			}
		}

		[Fact]
		public void CanIndexValuesForDictionaryAsPartOfDictionary()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User
					{
						Items = new Dictionary<string, string>
					        		        	{
					        		        		{"Color", "Red"}
					        		        	}
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Advanced.LuceneQuery<User>()
						.WhereEquals("Items,Key", "Color")
						.AndAlso()
						.WhereEquals("Items,Value", "Red")
						.ToArray();
					Assert.NotEmpty(users);
				}
			}
		}

		[Fact]
		public void CanIndexNestedValuesForDictionaryAsPartOfDictionary()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User
					        	{
						NestedItems = new Dictionary<string, NestedItem>
					    {
					        { "Color", new NestedItem{ Name="Red" } }
					    }
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Advanced.LuceneQuery<User>()
						.WhereEquals("NestedItems,Key", "Color")
						.AndAlso()
						.WhereEquals("NestedItems,Value.Name", "Red")
						.ToArray();
					Assert.NotEmpty(users);
				}
			}
		}

		[Fact]
		public void CanIndexValuesForIDictionaryAsPartOfIDictionary()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new UserWithIDictionary
					{
						Items = new Dictionary<string, string>
					        {
					            { "Color", "Red" }
					        }
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Advanced.LuceneQuery<UserWithIDictionary>()
						.WhereEquals("Items,Key", "Color")
						.AndAlso()
						.WhereEquals("Items,Value", "Red")
						.ToArray();
					Assert.NotEmpty(users);
				}
			}
		}

		[Fact]
		public void CanIndexNestedValuesForIDictionaryAsPartOfIDictionary()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new UserWithIDictionary
					{
						NestedItems = new Dictionary<string, NestedItem>
					    {
					        { "Color", new NestedItem{ Name="Red" } }
					    }
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Advanced.LuceneQuery<UserWithIDictionary>()
						.WhereEquals("NestedItems,Key", "Color")
						.AndAlso()
						.WhereEquals("NestedItems,Value.Name", "Red")
						.ToArray();
					Assert.NotEmpty(users);
				}
			}
		}

		[Fact]
		public void CanIndexValuesForDictionaryWithNumberForIndex()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User
					        	{
					        		Items = new Dictionary<string, string>
					        		        	{
					        		        		{"3", "Red"}
					        		        	}
					        	});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Advanced.LuceneQuery<User>()
						.WhereEquals("Items._3", "Red")
						.ToArray();
					Assert.NotEmpty(users);
				}
			}
		}

		#region Nested type: User / UserWithIDictionary / NestedItem

		public class User
		{
			public string Id { get; set; }
			public Dictionary<string, string> Items { get; set; }
			public Dictionary<string, NestedItem> NestedItems { get; set; }
		}

		public class UserWithIDictionary
		{
			public string Id { get; set; }
			public IDictionary<string, string> Items { get; set; }
			public IDictionary<string, NestedItem> NestedItems { get; set; }
		}

		public class NestedItem { public string Name { get; set; } }

		#endregion
	}
}