// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2719.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2719 : RavenTest
	{
		public class School
		{
			public string Id { get; set; }
			public List<User> Students { get; set; }
		}

		public class Students_ByNameCount : AbstractIndexCreationTask<School, Students_ByNameCount.Result>
		{
			public class Result
			{
				public string Name { get; set; }
				public int Count { get; set; }
			}

			public Students_ByNameCount()
			{
				Map = schools => from s in schools
					from student in s.Students
					select new
					{
						student.Name,
						Count = 1
					};

				Reduce = results => from result in results
					group result by result.Name
					into g
					select new
					{
						Name = g.Key,
						Count = g.Sum(x => x.Count)
					};
			}
		}

		[Fact]
		public void MapReduceIndexCanOutputMoreItemsPerDocumentAccordingToTheConfigurationLimit()
		{
			using (var store = NewDocumentStore())
			{
				var studentsByNameCount = new Students_ByNameCount();
				studentsByNameCount.Execute(store);

				using (var session = store.OpenSession())
				{
					var school = new School()
					{
						Students = new List<User>()
					};

					for (int i = 0; i < store.DocumentDatabase.Configuration.MaxMapReduceIndexOutputsPerDocument; i++)
					{
						school.Students.Add(new User()
						{
							Name = "Joe_" + i % 5,
						});
					}

					session.Store(school);
					session.SaveChanges();

					var results = session.Query<Students_ByNameCount.Result, Students_ByNameCount>().Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(5, results.Count);

					var stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == studentsByNameCount.IndexName);

					Assert.Equal(IndexingPriority.Normal, stats.Priority); // should not mark index as errored
				}
			}
		}
	}
}