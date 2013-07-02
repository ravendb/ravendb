// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Cache;
using System.Transactions;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WhenUsingDtcWeMustMakeSureToUpdateTheIndexingAboutNewWorkSoWeWontHaveInfiniteStaleIndexes : RavenTest
	{
		[Fact]
		public void MakeSureThatWeDoNotHaveTimeoutExceptionDueToStaleIndexes()
		{
			// This must run on ESENT to expose the failure
			using (var store = NewRemoteDocumentStore(databaseName: "Test", requestedStorage: "esent"))
			{
				for (var i = 1; i < 10; i++)
				{
					try
					{
						using (var scope = new TransactionScope())
						using (var conn = new SqlConnection(@"Data Source=.\sqlexpress;Initial Catalog=DB1;Integrated Security=True;"))
						using (var session = store.OpenSession())
						{
							conn.Open();

							var cmd = conn.CreateCommand();
							cmd.CommandText = "select top 1 id from TransactionTest";

							using (var reader = cmd.ExecuteReader())
								while (reader.Read())
									Console.WriteLine("test {0}", reader.GetInt32(0));

							session.Store(new Foo {Bar = "aaa"});
							session.SaveChanges();

							scope.Complete();
						}

						using (var session = store.OpenSession())
						{
							var count = session.Query<Foo>()
								.Customize(customization => customization.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
								.Count();
							Assert.Equal(i, count);
						}
					}
					catch (Exception)
					{
						Console.WriteLine(i);
						throw;
					}
				}
			}
		}

		class Foo
		{
			public string Bar { get; set; }
		}
	}
}