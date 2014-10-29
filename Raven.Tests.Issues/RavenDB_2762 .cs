// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2762 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2762 : RavenTest
	{
		[Fact]
		public void IndexingErrorsShouldSurviveDbRestart()
		{
			var dataDir = NewDataPath();
			IndexingError[] errors;
			using (var store = NewDocumentStore(runInMemory: false, dataDir: dataDir))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Names = new[] { "a", "b" } });
					session.SaveChanges();
				}
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs from name in doc.Names select new { name }",
					MaxIndexOutputsPerDocument = 1
				});

				WaitForIndexing(store);

				errors = store.DatabaseCommands.GetStatistics().Errors;

				Assert.NotEmpty(errors);
			}

			using (var store = NewDocumentStore(runInMemory: false, dataDir: dataDir))
			{
				var recoveredErrors = store.DatabaseCommands.GetStatistics().Errors;

				Assert.NotEmpty(recoveredErrors);

				Assert.Equal(errors.Length, recoveredErrors.Length);

				for (int i = 0; i < errors.Length; i++)
				{
					Assert.Equal(errors[i].Error, recoveredErrors[i].Error);
					Assert.Equal(errors[i].IndexName, recoveredErrors[i].IndexName);
				}
			}
		}
	}
}