using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Linq;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.MapRedue
{
	public class LetInReduceFunction : RavenTest
	{
		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class IndexWithLetInReduceFunction : AbstractIndexCreationTask<User, IndexWithLetInReduceFunction.ReduceResult>
		{
			public class ReduceResult
			{
				public string Id { get; set; }
				public string Name { get; set; }
			}

			public IndexWithLetInReduceFunction()
			{
				Map = users => from user in users
							   select new
										  {
											  user.Id,
											  user.Name
										  };

				Reduce = results => from result in results
									group result by result.Id
										into g
										let dummy = g.FirstOrDefault(x => x.Name != null)
										select new
												   {
													   Id = g.Key,
													   Name = dummy.Name
												   };
			}
		}

		[Fact]
		public void Can_perform_index_with_let_in_reduce_function()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Id = "users/ayende", Name = "Ayende Rahien" });
					session.Store(new User { Id = "users/dlang", Name = "Daniel Lang" });
					session.SaveChanges();
				}

				new IndexWithLetInReduceFunction().Execute(store);

				WaitForIndexing(store);

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}