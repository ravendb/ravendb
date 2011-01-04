//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class IndexingOnDictionary : LocalClientTest
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

		#region Nested type: User

		public class User
		{
			public string Id { get; set; }
			public Dictionary<string, string> Items { get; set; }
		}

		#endregion
	}
}