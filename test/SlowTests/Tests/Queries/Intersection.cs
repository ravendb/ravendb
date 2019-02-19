//-----------------------------------------------------------------------
// <copyright file="Intersection.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Tests.Queries
{
    public class IntersectionQuery : RavenTestBase
    {
        [Fact]
        public void CanPerformIntersectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                ExecuteTest(store);
            }
        }

        [Fact]
        public void CanPerformIntersectionQuery_Linq()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndexAndSampleData(store);

                using (var session = store.OpenSession())
                {
                    var shirts = session.Query<TShirt>("TShirtNested")
                        .OrderBy(x => x.BarcodeNumber)
                        .Where(x => x.Name == "Wolf")
                        .Intersect()
                        .Where(x => x.Types.Any(t => t.Color == "Blue" && t.Size == "Small"))
                        .Intersect()
                        .Where(x => x.Types.Any(t => t.Color == "Gray" && t.Size == "Large"))
                        .ToList();

                    Assert.Equal(6, shirts.Count);
                    Assert.True(shirts.All(x => x.Name == "Wolf"));
                    Assert.Equal(new[] { -999, 10001, 10002, 10003, 10004, 10006 }, shirts.Select(x => x.BarcodeNumber));
                }
            }
        }

        private void ExecuteTest(IDocumentStore store)
        {
            CreateIndexAndSampleData(store);

            using (var s = store.OpenSession())
            {
                //This should be BarCodeNumber = -999, 10001
                var resultPage1 = s.Advanced.DocumentQuery<TShirt>("TShirtNested")
                    .WhereEquals("Name", "Wolf")
                    .Intersect()
                    .WhereEquals("Types_Color", "Blue")
                    .AndAlso()
                    .WhereEquals("Types_Size", "Small")
                    .Intersect()
                    .WhereEquals("Types_Color", "Gray")
                    .AndAlso()
                    .WhereEquals("Types_Size", "Large")
                    .OrderBy("BarcodeNumber")
                    .Take(2)
                    .ToList();
                Assert.Equal(2, resultPage1.Count);
                Assert.True(resultPage1.All(x => x.Name == "Wolf"));
                foreach (var result in resultPage1)
                {
                    Assert.True(result.Types.Any(x => x.Color == "Gray" && x.Size == "Large"));
                    Assert.True(result.Types.Any(x => x.Color == "Blue" && x.Size == "Small"));
                }
                Assert.Equal(new[] { -999, 10001 }, resultPage1.Select(r => r.BarcodeNumber));

                //This should be BarCodeNumber = 10001, 10002(i.e.it spans pages 1 & 2)
                var resultPage1a = s.Advanced.DocumentQuery<TShirt>("TShirtNested")
                    .WhereEquals("Name", "Wolf")
                    .Intersect()
                    .WhereEquals("Types_Color", "Blue")
                    .AndAlso()
                    .WhereEquals("Types_Size", "Small")
                    .Intersect()
                    .WhereEquals("Types_Color", "Gray")
                    .AndAlso()
                    .WhereEquals("Types_Size", "Large")
                    .OrderBy("BarcodeNumber")
                    .Skip(1)
                    .Take(2)
                    .ToList();
                Assert.Equal(2, resultPage1a.Count);
                Assert.True(resultPage1a.All(x => x.Name == "Wolf"));
                foreach (var result in resultPage1a)
                {
                    Assert.True(result.Types.Any(x => x.Color == "Gray" && x.Size == "Large"));
                    Assert.True(result.Types.Any(x => x.Color == "Blue" && x.Size == "Small"));
                }
                Assert.Equal(new[] { 10001, 10002 }, resultPage1a.Select(r => r.BarcodeNumber));

                //This should be BarCodeNumber = 10002, 10003, 10004, 10006 (But NOT 10005
                var resultPage2 = s.Advanced.DocumentQuery<TShirt>("TShirtNested")
                    //.Where("Name:Wolf INTERSECT Types_Color:Blue AND Types_Size:Small INTERSECT Types_Color:Gray AND Types_Size:Large")
                    .WhereEquals("Name", "Wolf")
                    .Intersect()
                    .WhereEquals("Types_Color", "Blue")
                    .AndAlso()
                    .WhereEquals("Types_Size", "Small")
                    .Intersect()
                    .WhereEquals("Types_Color", "Gray")
                    .AndAlso()
                    .WhereEquals("Types_Size", "Large")
                    .OrderBy("BarcodeNumber")
                    .Skip(2)
                    .Take(10) //we should only get 4 here, want to test a page size larger than what is possible!!!!!
                    .ToList();
                Assert.Equal(4, resultPage2.Count);
                Assert.True(resultPage2.All(x => x.Name == "Wolf"));
                foreach (var result in resultPage2)
                {
                    Assert.True(result.Types.Any(x => x.Color == "Gray" && x.Size == "Large"));
                    Assert.True(result.Types.Any(x => x.Color == "Blue" && x.Size == "Small"));
                }
                Assert.Equal(new[] { 10002, 10003, 10004, 10006 }, resultPage2.Select(r => r.BarcodeNumber));
            }
        }

        private void CreateIndexAndSampleData(IDocumentStore store)
        {
            using (var s = store.OpenSession())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                                                new IndexDefinition
                                                {
                                                    Name = "TShirtNested",
                                                    Maps =
                                                    {
                                                        @"from s in docs.TShirts
                                                            from t in s.Types
                                                            select new { s.Name, Types_Color = t.Color, Types_Size = t.Size, s.BarcodeNumber }"
                                                    },
                                                    Fields = new Dictionary<string, IndexFieldOptions>
                                                    {
                                                        { "BarcodeNumber", new IndexFieldOptions { } }
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

        private IEnumerable<TShirt> GetSampleData()
        {
            var tShirt1 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = 10001,
                Types = new List<TShirtType>
                    {
                        new TShirtType { Color = "Blue",  Size = "Small" },
                        new TShirtType { Color = "Black", Size = "Small" },
                        new TShirtType { Color = "Black", Size = "Medium" },
                        new TShirtType { Color = "Gray",  Size = "Large" }
                    }
            };
            var tShirt2 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = 1,
                Types = new List<TShirtType>
                                {
                                    new TShirtType { Color = "Blue",  Size = "Small" },
                                    new TShirtType { Color = "Black", Size = "Large" },
                                    new TShirtType { Color = "Gray",  Size = "Medium" }
                                }
            };
            var tShirt3 = new TShirt
            {
                Name = "Owl",
                BarcodeNumber = 99999,
                Types = new List<TShirtType>
                                {
                                    new TShirtType { Color = "Blue",  Size = "Small" },
                                    new TShirtType { Color = "Gray",  Size = "Medium" }
                                }
            };
            var tShirt4 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = -999,
                Types = new List<TShirtType>
                    {
                        new TShirtType { Color = "Blue",  Size = "Small" },
                        new TShirtType { Color = "Black", Size = "Small" },
                        new TShirtType { Color = "Black", Size = "Medium" },
                        new TShirtType { Color = "Gray",  Size = "Large" }
                    }
            };
            var tShirt5 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = 10002,
                Types = new List<TShirtType>
                                {
                                    new TShirtType { Color = "Blue",  Size = "Small" },
                                    new TShirtType { Color = "Gray",  Size = "Large" }
                                }
            };
            var tShirt6 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = 10003,
                Types = new List<TShirtType>
                                {
                                    new TShirtType { Color = "Blue",  Size = "Small" },
                                    new TShirtType { Color = "Gray",  Size = "Large" }
                                }
            };
            var tShirt7 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = 10004,
                Types = new List<TShirtType>
                                {
                                    new TShirtType { Color = "Blue",  Size = "Small" },
                                    new TShirtType { Color = "Gray",  Size = "Large" }
                                }
            };

            var tShirt8 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = 10005,
                Types = new List<TShirtType> //Doesn't MAtch SUB-QUERIES
                                {
                                    new TShirtType { Color = "Blue",  Size = "Small" },
                                    new TShirtType { Color = "Gray",  Size = "Medium" }
                                }
            };

            var tShirt9 = new TShirt
            {
                Name = "Wolf",
                BarcodeNumber = 10006,
                Types = new List<TShirtType>
                                {
                                    new TShirtType { Color = "Blue",  Size = "Small" },
                                    new TShirtType { Color = "Gray",  Size = "Large" }
                                }
            };

            yield return tShirt1;
            yield return tShirt2;
            yield return tShirt3;
            yield return tShirt4;
            yield return tShirt5;
            yield return tShirt6;
            yield return tShirt7;
            yield return tShirt8;
            yield return tShirt9;
        }

        private class TShirt
        {
            public String Id { get; set; }
            public String Name { get; set; }
            public int BarcodeNumber { get; set; }
            public List<TShirtType> Types { get; set; }
        }

        private class TShirtType
        {
            public String Color { get; set; }
            public String Size { get; set; }

            public override string ToString()
            {
                return String.Format("{{{0}, {1}}}", Color, Size);
            }
        }
    }
}
