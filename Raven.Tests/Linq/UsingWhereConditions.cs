//-----------------------------------------------------------------------
// <copyright file="UsingWhereConditions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Tests.Common;

using Xunit;

/*
 * Different test using where clause
 */
namespace Raven.Tests.Linq
{
	public class UsingWhereConditions : RavenTest
	{
		[Fact]
		public void Can_Use_Where()
		{
			using (var db = NewDocumentStore())
			{
				const string indexName = "CommitByRevision";
				using (var session = db.OpenSession())
	            {
					AddData(session);                    

					db.DatabaseCommands.DeleteIndex(indexName);
					var result = db.DatabaseCommands.PutIndex(indexName,
							new IndexDefinitionBuilder<CommitInfo, CommitInfo>
							{
								Map = docs => from doc in docs
											  select new { doc.Revision},
							}, true);                    

					WaitForQueryToComplete(session, indexName);

					var Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision == 1);
					//There is one CommitInfo with Revision == 1
					Assert.Equal(1, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision == 0);
					//There is not CommitInfo with Revision = 0 so hopefully we do not get any result
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision < 1 );
					//There are 0 CommitInfos which has Revision <1 
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision <  2);
					//There is one CommitInfo with Revision < 2
					Assert.Equal(1, Results.ToArray().Count());
					//Revision of resulted CommitInfo has to be 1
					var cinfo = Results.ToArray()[0];
					Assert.Equal(1, cinfo.Revision);

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision <= 2);
					//There are 2 CommitInfos which has Revision <=2 
					Assert.Equal(2, Results.ToArray().Count());


					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision > 7);
					//There are 0 CommitInfos which has Revision >7 
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision > 6);
					//There are 1 CommitInfos which has Revision >6 
					Assert.Equal(1, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision >= 6);
					//There are 2 CommitInfos which has Revision >=6 
					Assert.Equal(2, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision > 6 && x.Revision < 6);
					//There are 0 CommitInfos which has Revision >6 && <6 
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision >= 6 && x.Revision <= 6);
					//There are 1 CommitInfos which has Revision >=6 && <=6 
					Assert.Equal(1, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision >= 6 && x.Revision < 6);
					//There are 0 CommitInfos which has Revision >=6 && <6 
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision > 6 && x.Revision <= 6);
					//There are 0 CommitInfos which has Revision >6 && <=6 
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision >= 7 && x.Revision <= 1);
					//There are 0 CommitInfos which has Revision >=7  && <= 1 
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision > 7 && x.Revision < 1);
					//There are 0 CommitInfos which has Revision >7  && < 1 
					Assert.Equal(0, Results.ToArray().Count());


					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision > 7 || x.Revision < 1);
					//There are 0 CommitInfos which has Revision >7  || < 1 
					Assert.Equal(0, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision >= 7 || x.Revision < 1);
					//There are 1 CommitInfos which has Revision >=7  || < 1 
					Assert.Equal(1, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision > 7 || x.Revision <= 1);
					//There are 1 CommitInfos which has Revision >7  || <= 1 
					Assert.Equal(1, Results.ToArray().Count());

					Results = session.Query<CommitInfo>(indexName)
											.Where(x => x.Revision >= 7 || x.Revision <= 1);
					//There are 2 CommitInfos which has Revision >=7  || <= 1 
					Assert.Equal(2, Results.ToArray().Count());
	            }
			}
		}

		private static string Repo = "/svn/repo/";
		private static void WaitForQueryToComplete(IDocumentSession session, string indexName)
		{            
			QueryResult results;
			do
			{
				//doesn't matter what the query is here, just want to see if it's stale or not
                results = session.Advanced.DocumentQuery<CommitInfo>(indexName)                              
							  .Where("") 
							  .QueryResult;   

				if (results.IsStale)
					Thread.Sleep(1000);
			} while (results.IsStale);            
		}

		private void AddData(IDocumentSession documentSession)
		{
			documentSession.Store(new CommitInfo { Author="kenny", PathInRepo="/src/test/", Repository=Repo, Revision=1, Date= SystemTime.UtcNow, CommitMessage="First commit" });
			documentSession.Store(new CommitInfo { Author = "kenny", PathInRepo = "/src/test/FirstTest/", Repository = Repo, Revision = 2, Date = SystemTime.UtcNow, CommitMessage = "Second commit" });
			documentSession.Store(new CommitInfo { Author = "kenny", PathInRepo = "/src/test/FirstTest/test.txt", Repository = Repo, Revision = 3, Date = SystemTime.UtcNow, CommitMessage = "Third commit" });
			documentSession.Store(new CommitInfo { Author = "john", PathInRepo = "/src/test/SecondTest/", Repository = Repo, Revision = 4, Date = SystemTime.UtcNow, CommitMessage = "Fourth commit" });
			documentSession.Store(new CommitInfo { Author = "john", PathInRepo = "/src/", Repository = Repo, Revision = 5, Date = SystemTime.UtcNow, CommitMessage = "Fifth commit" });
			documentSession.Store(new CommitInfo { Author = "john", PathInRepo = "/src/test/SecondTest/test.txt", Repository = Repo, Revision = 6, Date = SystemTime.UtcNow, CommitMessage = "Sixth commit" });
			documentSession.Store(new CommitInfo { Author = "kenny", PathInRepo = "/src/test/SecondTest/test1.txt", Repository = Repo, Revision = 7, Date = SystemTime.UtcNow, CommitMessage = "Seventh commit" });
			documentSession.SaveChanges();
		}

        public class CommitInfo
        {
            public string Id { get; set; }
            public string Author { get; set; }
            public string PathInRepo { get; set; }
            public string Repository { get; set; }
            public int Revision { get; set; }
            public DateTime Date { get; set; }
            public string CommitMessage { get; set; }
        }
	}
}