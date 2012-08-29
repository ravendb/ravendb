//-----------------------------------------------------------------------
// <copyright file="Intersection.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Xunit;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;

namespace Raven.Tests.Queries
{   
	public class IntersectionQueryWithLargeDataset : RavenTest
	{
		[Fact]
		public void CanPerformIntersectionQuery_Remotely()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				ExecuteTest(store);
			}
		}

		[Fact]
		public void CanPeformIntersectionQuery_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteTest(store);
			}
		}

		private void ExecuteTest(IDocumentStore store)
		{
			CreateIndexAndSampleData(store);

			// there are 10K documents, each combination of "Lorem" and "Nullam" has 100 matching documents.
			// Suspect that this may be failing because each individual slice (Lorem: L and Nullam: N)
			// has 1000 documents, which is greater than default page size of 128.
			foreach (string L in Lorem)
			{
				foreach (string N in Nullam)
				{
					using (var session = store.OpenSession())
					{
						var result = session.Query<TestAttributes>("TestAttributesByAttributes")
									.Where(o => o.Attributes.Any(t => t.Key == "Lorem" && t.Value == L))
									.OrderBy(o => o.Id)
									.Intersect()
									.Where(o => o.Attributes.Any(t => t.Key == "Nullam" && t.Value == N))
									.ToList();

						Assert.Equal(100, result.Count);
					}
				}
			}
		}

		private void CreateIndexAndSampleData(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				store.DatabaseCommands.PutIndex("TestAttributesByAttributes",
													new IndexDefinition
													{
														Map =
														@"from e in docs.TestAttributes
															from r in e.Attributes
															select new { Attributes_Key = r.Key, Attributes_Value = r.Value }"
													});

				foreach (var sample in GetSampleData())
				{
					s.Store(sample);
				}
				s.SaveChanges();
			}

			WaitForIndexing(store);
		}

		readonly string[] Lorem = { "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "Sed", "auctor", "erat" };
		readonly string[] Nullam = { "nec", "quam", "id", "risus", "congue", "bibendum", "Nam", "lacinia", "eros", "quis" };
		readonly string[] Quisque = { "varius", "rutrum", "magna", "posuere", "urna", "sollicitudin", "Integer", "libero", "lacus", "tincidunt" };
		readonly string[] Aliquam = { "erat", "volutpat", "placerat", "interdum", "felis", "luctus", "quam", "sagittis", "mattis", "Proin" };

		private IEnumerable<TestAttributes> GetSampleData()
		{
			List<TestAttributes> result = new List<TestAttributes>();

			foreach (string L in Lorem)
			{
				foreach (string N in Nullam)
				{
					foreach (string Q in Quisque)
					{
						foreach (string A in Aliquam)
						{
							TestAttributes t = new TestAttributes { Attributes = new Dictionary<string, string>(), val = 1 };
							t.Attributes.Add("Lorem", L);
							t.Attributes.Add("Nullam", N);
							t.Attributes.Add("Quisque", Q);
							t.Attributes.Add("Aliquam", A);
							result.Add(t);
						}
					}
				}
			}
			return result;
		}

		public class TestAttributes
		{
			public string Id { get; set; }
			public Dictionary<string, string> Attributes { get; set; }
			public int val { get; set; }
		}
	}
}
