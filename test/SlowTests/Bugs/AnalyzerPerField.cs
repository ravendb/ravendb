//-----------------------------------------------------------------------
// <copyright file="AnalyzerPerField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Lucene.Net.Analysis;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace SlowTests.Bugs
{
    public class AnalyzerPerField : RavenTestBase
    {
        [Fact]
        public void CanUseAnalyzerPerField()
        {
            using (var store = GetDocumentStore())
            {

                store.Admin.Send(new PutIndexesOperation(new [] { new IndexDefinition
                {
                  Maps = { "from movie in docs.Movies select new { movie.Name, movie.Tagline}" },
                    Name = "Movies",
                  Fields = {{ "Name" , new IndexFieldOptions {Analyzer = typeof (SimpleAnalyzer).FullName}} ,
                            { "Tagline", new IndexFieldOptions { Analyzer = typeof(StopAnalyzer).FullName}}}                 
                 }}));
     
                using (var s = store.OpenSession())
                {
                    s.Store(new Movie
                    {
                        Name = "Hello Dolly",
                        Tagline = "She's a jolly good"
                    });
                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var movies = s.Advanced.DocumentQuery<Movie>("Movies")
                        .WhereLucene("Name", "DOLLY")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(1, movies.Count);

                    movies = s.Advanced.DocumentQuery<Movie>("Movies")
                        .WhereLucene("Name", "she's")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(1, movies.Count);
                }
            }
        }

        private class Movie
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Tagline { get; set; }
        }
    }
}
