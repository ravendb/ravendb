using System.Collections.Generic;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class IndexingOnDictionary : LocalClientTest
	{
		public class User
		{
			public string Id { get; set; }
			public Dictionary<string, string> Items { get; set; }
		}

		[Fact]
		public void CanIndexValuesForDictionary()
		{
			using (var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
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

	}
}