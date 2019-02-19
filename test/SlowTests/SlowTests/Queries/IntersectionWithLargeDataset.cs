//-----------------------------------------------------------------------
// <copyright file="Intersection.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.SlowTests.Queries
{
    public class IntersectionQueryWithLargeDataset : RavenTestBase
    {
        [Fact]
        public void CanPerformIntersectionQuery_Embedded()
        {
            using (var store = GetDocumentStore())
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
            foreach (string L in _lorem)
            {
                foreach (string N in _nullam)
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<TestAttributes>("TestAttributesByAttributes")
                                    .Where(o => o.Attributes.Any(t => t.Key == "Lorem" && t.Value == L))
                                    .OrderBy(o => o.Id)
                                    .Intersect()
                                    .Where(o => o.Attributes.Any(t => t.Key == "Nullam" && t.Value == N))
                                    .Take(128)
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
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Name = "TestAttributesByAttributes",
                        Maps =
                        {
                          @"from e in docs.TestAttributes
                            from r in e.Attributes
                            select new { Attributes_Key = r.Key, Attributes_Value = r.Value }"
                        }
                    }}));

                foreach (var sample in GetSampleData())
                {
                    s.Store(sample);
                }
                s.SaveChanges();
            }

            WaitForIndexing(store);
        }

        private readonly string[] _lorem = { "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "Sed", "auctor", "erat" };
        private readonly string[] _nullam = { "nec", "quam", "id", "risus", "congue", "bibendum", "Nam", "lacinia", "eros", "quis" };
        private readonly string[] _quisque = { "varius", "rutrum", "magna", "posuere", "urna", "sollicitudin", "Integer", "libero", "lacus", "tincidunt" };
        private readonly string[] _aliquam = { "erat", "volutpat", "placerat", "interdum", "felis", "luctus", "quam", "sagittis", "mattis", "Proin" };

        private IEnumerable<TestAttributes> GetSampleData()
        {
            List<TestAttributes> result = new List<TestAttributes>();

            foreach (string L in _lorem)
            {
                foreach (string N in _nullam)
                {
                    foreach (string Q in _quisque)
                    {
                        foreach (string A in _aliquam)
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

        private class TestAttributes
        {
            public string Id { get; set; }
            public Dictionary<string, string> Attributes { get; set; }
            public int val { get; set; }
        }
    }
}
