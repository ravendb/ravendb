//-----------------------------------------------------------------------
// <copyright file="Intersection.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Linq;
using Xunit;
using Raven.Abstractions.Indexing;
using System.Threading;
using System.Linq.Expressions;
using Raven.Client.Document;

namespace Raven.Tests.Queries
{   
    public class IntersectionQuery : RavenTest
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
            var tShirt1 = new TShirt
                {
                    Name = "Wolf",
                    BarcodeNumber = "10001",
                    Types = new List<TShirtType>
                    {
                        new TShirtType { Colour = "Blue",  Size = "Small" },
                        new TShirtType { Colour = "Black", Size = "Small" },
                        new TShirtType { Colour = "Black", Size = "Medium" },
                        new TShirtType { Colour = "Gray",  Size = "Large" }
                    }
                };
            var tShirt2 = new TShirt
                {
                    Name = "Wolf",
                    BarcodeNumber = "01234",
                    Types = new List<TShirtType>
                                {
                                    new TShirtType { Colour = "Blue",  Size = "Small" },                                    
                                    new TShirtType { Colour = "Black", Size = "Large" },
                                    new TShirtType { Colour = "Gray",  Size = "Medium" }
                                }
                };
            var tShirt3 = new TShirt
                {
                    Name = "Owl",
                    BarcodeNumber = "99999",
                    Types = new List<TShirtType>
                                {
                                    new TShirtType { Colour = "Blue",  Size = "Small" },
                                    new TShirtType { Colour = "Gray",  Size = "Medium" }
                                }
                };

            using (var s = store.OpenSession())
            {
                store.DatabaseCommands.PutIndex("TShirtNested",
                                                new IndexDefinition
                                                {
                                                    Map = @"from s in docs.TShirts
                                                            from t in s.Types
                                                            select new { s.Name, t.Colour, t.Size }"
                                                });

                s.Store(tShirt1);
                s.Store(tShirt2);
                s.Store(tShirt3);
                s.SaveChanges();
            }

			WaitForIndexing(store);

            using (var s = store.OpenSession())
            {
                var results = s.Advanced.LuceneQuery<TShirt>("TShirtNested")                             
                    .Where("Name:Wolf INTERSECT Colour:Blue AND Size:Small INTERSECT Colour:Gray AND Size:Large")
                    .ToList();
                
                Assert.True(results.All(x => x.Name == "Wolf"));
                foreach (var result in results)
                {
                    Assert.True(result.Types.Any(x => x.Colour == "Gray" && x.Size == "Large"));
                    Assert.True(result.Types.Any(x => x.Colour == "Blue" && x.Size == "Small"));
                }
            }
        }

        public class TShirt
        {
            public String Id { get; set; }
            public String Name { get; set; }
            public String BarcodeNumber { get; set; }
            public List<TShirtType> Types { get; set; }
        }

        public class TShirtType
        {
            public String Colour { get; set; }
            public String Size { get; set; }

            public override string ToString()
            {
                return String.Format("{{{0}, {1}}}", Colour, Size);
            }
        }       
    }    
}
