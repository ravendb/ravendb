// -----------------------------------------------------------------------
//  <copyright file="Tamir.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Tamir : RavenTest
	{
		public class Developer
		{
			public string Name { get; set; }
			public IDE PreferredIDE { get; set; }
		}

		public class IDE
		{
			public string Name { get; set; }
		}

		[Fact]
		public void InOnObjects()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.DatabaseCommands.PutIndex("DevByIDE", new IndexDefinition
				{
					Map = @"from dev in docs.developers select new { dev.PreferredIDE, dev.PreferredIDE.Name }"
				});

				using (var session = store.OpenSession())
				{

					IEnumerable<Developer> developers = from name in new[] { "VisualStudio", "Vim", "Eclipse", "PyCharm" }
														select new Developer
														{
															Name = string.Format("Fan of {0}", name),
															PreferredIDE = new IDE
															{
																Name = name
															}
														};

					foreach (var developer in developers)
					{
						session.Store(developer);
					}

					session.SaveChanges();

				}

				using (var session = store.OpenSession())
				{
					var bestIDEsEver = new[] { new IDE { Name = "VisualStudio" }, new IDE { Name = "IntelliJ" } };

					RavenQueryStatistics stats;
					// this query returns with results
					var querySpecificIDE = session.Query<Developer>().Statistics(out stats)
												  .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
												  .Where(d => d.PreferredIDE.Name == "VisualStudio")
												  .ToList();

					Assert.NotEmpty(querySpecificIDE);

					// this query returns empty
					var queryUsingWhereIn = session.Query<Developer>("DevByIDE").Statistics(out stats)
												   .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
												   .Where(d => d.PreferredIDE.In(bestIDEsEver))
												   .ToList();
					Assert.NotEmpty(queryUsingWhereIn);
				}
			}
		}

	}
}