//-----------------------------------------------------------------------
// <copyright file="FacetedIndex.cs" company="Hibernating Rhinos LTD">
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
    public class TShirt
    {
        public String Id { get; set; }
        public String Name { get; set; }
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
            using (var s = store.OpenSession())
            {
                store.DatabaseCommands.PutIndex("TShirtNested",
                                                new IndexDefinition
                                                {
                                                    Map = @"from s in docs.TShirts
                                                            from t in s.Types
                                                            select new { s.Name, t.Colour, t.Size }"

                                                    //Map = @"from s in docs.TShirts
                                                    //        select new 
                                                    //                { 
                                                    //                    s.Name, 
                                                    //                    Color = s.Types.Select(x=>x.Color),
                                                    //                    Size = s.Types.Select(x=>x.Size) 
                                                    //                }"
                                                });

                s.Store(new TShirt
                            {
                                Name = "TShirt we want",
                                Types = new List<TShirtType>
                                {
                                    new TShirtType { Colour = "Blue",  Size = "Small" },
                                    new TShirtType { Colour = "Black", Size = "Small" },
                                    new TShirtType { Colour = "Black", Size = "Medium" },
                                    new TShirtType { Colour = "Gray",  Size = "Large" }
                                }        
                            });
                s.Store(new TShirt
                {
                    Name = "TShirt we DON'T want",
                    Types = new List<TShirtType>
                                {
                                    new TShirtType { Colour = "Blue",  Size = "Small" },                                    
                                    new TShirtType { Colour = "Black", Size = "Large" },
                                    new TShirtType { Colour = "Gray",  Size = "Medium" }
                                }
                });
                s.SaveChanges();

                //Force the index to update
                var results = s.Query<TShirt>("TShirtNested")
                                .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                                .ToList();
            }

            using (var s = store.OpenSession())
            {
                //Existing queries can only handle an OR here, what we want is AND or INTERSECT
                var existingResults = s.Advanced.LuceneQuery<TShirt>("TShirtNested")                             
                   .Where("(Colour:Blue AND Size:Small) OR (Colour:Gray AND Size:Large)")
                   .ToList();

                //Sample query "Name:Wolf INTERSECT Color:Blue AND Size:Small INTERSECT Color:Gray AND Size:Large"
                var results = s.Advanced.LuceneQuery<TShirt>("TShirtNested")                             
                    .Where("Colour:Blue AND Size:Small INTERSECT Colour:Gray AND Size:Large")
                    .ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal("TShirt we want", results[0].Name);      
            }
        }        

        private static void Log(string text, params object[] args)
        {
            Trace.WriteLine(String.Format(text, args));
            Console.WriteLine(text, args);
        }

        private static double TimeIt(Action action)
        {
            var timer = Stopwatch.StartNew();
            action();
            timer.Stop();
            return timer.ElapsedMilliseconds;
        }
    }    
}
