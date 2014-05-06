using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class ComplexIndexes : RavenTest
	{
		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
			
		}

		[Fact]
		public void CanCreateIndex()
		{
			using(var store = NewDocumentStore())
			{
				new ReadingHabits_ByDayOfWeek_MultiMap().Execute(store);
			}
		}

		public class ReadingHabits_ByDayOfWeek_MultiMap
		 : AbstractMultiMapIndexCreationTask<ReadingHabits_ByDayOfWeek_MultiMap.Result>
		{
			public class Result
			{
				public string UserId { get; set; }
				public CountPerDay[] CountsPerDay { get; set; }
				public string Name { get; set; }

				public class CountPerDay
				{
					public DayOfWeek DayOfWeek { get; set; }
					public int Count { get; set; }
				}
			}
			public ReadingHabits_ByDayOfWeek_MultiMap()
			{
				AddMap<ReadingList>(lists =>
					  from list in lists
					  select new
					  {
						  list.UserId,
						  Name = (string)null,
						  CountsPerDay = from b in list.Books
										 group b by b.ReadAt.DayOfWeek into g
										 select new
										 {
											 DayOfWeek = g.Key,
											 Count = g.Count()
										 }
					  });

				AddMap<User>(users =>
				             users.SelectMany(user => Enumerable.Range(0, 6), (user, day) => new
				             {
				             	UserId = user.Id,
				             	CountsPerDay = new object[0],
				             	user.Name,
				             })
					);

				Reduce = results =>
						 from result in results
						 group result by result.UserId
							 into g
							 select new
							 {
								 UserId = g.Key,
								 Name = g.Select(x => x.Name).FirstOrDefault(x => x != null),
								 CountsPerDay = g.SelectMany(x => x.CountsPerDay).GroupBy(cpd => cpd.DayOfWeek).Select(gi => new
								 {
									 DayOfWeek = gi.Key,
									 Count = gi.Sum(x => x.Count)
								 })
							 };
			}
		}


		public class ReadingList
		{
			public string Id { get; set; }
			public string UserId { get; set; }

			public List<ReadBook> Books { get; set; }

			public class ReadBook
			{
				public string Title { get; set; }
				public DateTime ReadAt { get; set; }
			}
		}

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}
