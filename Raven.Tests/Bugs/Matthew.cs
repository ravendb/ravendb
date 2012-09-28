using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.Bugs
{
	public class Matthew : RavenTest
	{
		[Fact]
		public void CanUseLuceneQueryToQueryIndex()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			{
				DefaultDatabase = "TESTS",
				Url = "http://localhost:8079"
			}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("TESTS");
				store.Conventions.FindTypeTagName = type =>
				{
					if (typeof(Oil).IsAssignableFrom(type))
						return "oils";
					if (typeof(User).IsAssignableFrom(type))
						return "users";
					if (typeof(Feedback).IsAssignableFrom(type))
						return "feedback";
					return "unknown_add_to_global.asax.cs";
				};
				new PopularOilsIndex().Execute(store);



				using(var sesison = store.OpenSession())
				{
					var popularOilsResults = sesison.Query<PopularOilsResult, PopularOilsIndex>()
								  .Customize(x => x.WaitForNonStaleResultsAsOfNow())
								  .OrderByDescending(x => x.Count).Take(10)
								  .ToList();
				}

			}
		}

		public class PopularOilsIndex : AbstractIndexCreationTask<User, PopularOilsResult>
		{
			public PopularOilsIndex()
			{
				Map = users => from user in users
							   from oil in user.Favorites
							   select new { OilId = oil, Count = 1 };
				Reduce = results => from result in results
									group result by result.OilId into g
									select new
									{
										OilId = g.Key,
										Count = g.Sum(x => x.Count)
									};
			}
		}


		public class User
		{
			public Oil[] Favorites { get; set; }
		}
		public class PopularOilsResult
		{
			public string OilId { get; set; }
			public int Count { get; set; }
		}
		public class Oil
		{
		}


		public class Feedback
		{
		}
	}

}