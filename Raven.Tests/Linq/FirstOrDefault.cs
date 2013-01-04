using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Linq
{
	public class FirstOrDefault : RavenTest
	{
		private class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		private class IndexWithLetInReduceFunction : AbstractIndexCreationTask<User, IndexWithLetInReduceFunction.ReduceResult>
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
				                    let dummy = g.First(x => x.Name != null)
				                    select new
				                    {
					                    Id = g.Key,
										dummy.Name,
				                    };
			}
		}

		[Fact]
		public void WillReplaceFirstWithFirstOrDefault()
		{
			var indexDefinition = new IndexWithLetInReduceFunction {Conventions = new DocumentConvention()}.CreateIndexDefinition();
			Assert.Contains("FirstOrDefault", indexDefinition.Reduce);
		}
	}
}