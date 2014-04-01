using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class StreamingTests : RavenTest
	{
		public class UserFull
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Description { get; set; }
		}

		public class UserLightweight
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void CanStreamUsingLuceneSelectFields()
		{
			int count = 0;
			using (var store = NewDocumentStore())
			{
				new UserIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					for (var i = 0; i < 10; i++)
					{
						session.Store(new UserFull {Name = "Name " + i, Description = "Description " + i});
					}
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
                    var query = session.Advanced.DocumentQuery<UserFull, UserIndex>().SelectFields<UserLightweight>();

					using (var reader = session.Advanced.Stream(query))
					{
						while (reader.MoveNext())
						{
							count++;
							Assert.IsType<UserLightweight>(reader.Current.Document);
						}
					}
				}
				Assert.Equal(10, count);
			}
		}

        [Fact]
        public void CanGetUsingLuceneSelectFields()
        {
            int count = 0;
            using (var store = NewDocumentStore())
            {
                new UserIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new UserFull { Name = "Name " + i, Description = "Description " + i });
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<UserFull, UserIndex>().SelectFields<UserLightweight>();

                    using (var reader = query.GetEnumerator())
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<UserLightweight>(reader.Current);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }

		class UserIndex : AbstractIndexCreationTask<UserFull>
		{
			public UserIndex()
			{
				Map = users => from u in users select new { u.Name };
			}
		}
	}

}