// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1018.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Net;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1018 : RavenTest
	{
		public class SimpleIndex : AbstractIndexCreationTask<SimpleClass>
		{
			public SimpleIndex()
			{
				this.Map = results => from r in results
									  select new
									  {
										  r.Date,
										  r.Name,
										  r.Value
									  };
			}
		}

		public class ComplexIndex : AbstractIndexCreationTask<ComplexClass>
		{
			public ComplexIndex()
			{
				this.Map = results => from r in results
				                      select new
				                      {
					                      r.Field,
					                      r.Class.Date,
					                      r.Class.Name,
					                      r.Class.Value
				                      };
			}
		}

		public class SimpleClass
		{
			public string Name { get; set; }

			public int Value { get; set; }

			public DateTime Date { get; set; }
		}

		public class ComplexClass
		{
			public string Field { get; set; }

			public SimpleClass Class { get; set; }
		}

		[Fact]
		public void SimpleCsvTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var now = new DateTime(2000, 5, 10);

				new SimpleIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new SimpleClass
					{
						Date = now.AddDays(-1),
						Name = "name1",
						Value = 1
					});

					session.Store(new SimpleClass
					{
						Date = now.AddDays(-2),
						Name = "name2",
						Value = 2
					});

					session.Store(new SimpleClass
					{
						Date = now.AddDays(-3),
						Name = "name3",
						Value = 3
					});

					session.SaveChanges();

					var results = session
						.Query<SimpleClass>("SimpleIndex")
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();

					Assert.Equal(3, results.Count);
				}

				var csvString = new WebClient().DownloadString(string.Format("http://localhost:8079/databases/{0}/streams/query/SimpleIndex?format=excel", store.DefaultDatabase));

				Assert.Equal("Name,Value,Date,\r\nname1,1,2000-05-09T00:00:00.0000000,\r\nname2,2,2000-05-08T00:00:00.0000000,\r\nname3,3,2000-05-07T00:00:00.0000000,\r\n", csvString);

				csvString = new WebClient().DownloadString(string.Format("http://localhost:8079/databases/{0}/streams/query/SimpleIndex?format=excel&query=Value:1", store.DefaultDatabase));

				Assert.Equal("Name,Value,Date,\r\nname1,1,2000-05-09T00:00:00.0000000,\r\n", csvString);
			}
		}

		[Fact]
		public void ComplexCsvTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var now = new DateTime(2000, 5, 10);

				new ComplexIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new ComplexClass
					{
						Class = new SimpleClass
						{
							Date = now.AddDays(-1),
							Name = "name1",
							Value = 1
						},
						Field = "field1"
					});

					session.Store(new ComplexClass
					{
						Class = new SimpleClass
						{
							Date = now.AddDays(-2),
							Name = "name2",
							Value = 2
						},
						Field = "field2"
					});

					session.Store(new ComplexClass
					{
						Class = new SimpleClass
						{
							Date = now.AddDays(-3),
							Name = "name3",
							Value = 3
						},
						Field = "field3"
					});

					session.SaveChanges();

					var results = session
						.Query<ComplexClass>("ComplexIndex")
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();

					Assert.Equal(3, results.Count);
				}

				var csvString = new WebClient().DownloadString(string.Format("http://localhost:8079/databases/{0}/streams/query/ComplexIndex?format=excel", store.DefaultDatabase));

				Assert.Equal("Field,Class.Name,Class.Value,Class.Date,\r\nfield1,name1,1,2000-05-09T00:00:00.0000000,\r\nfield2,name2,2,2000-05-08T00:00:00.0000000,\r\nfield3,name3,3,2000-05-07T00:00:00.0000000,\r\n", csvString);

				csvString = new WebClient().DownloadString(string.Format("http://localhost:8079/databases/{0}/streams/query/ComplexIndex?format=excel&query=Value:1", store.DefaultDatabase));

				Assert.Equal("Field,Class.Name,Class.Value,Class.Date,\r\nfield1,name1,1,2000-05-09T00:00:00.0000000,\r\n", csvString);
			}
		}
	}
}