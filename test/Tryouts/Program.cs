using System;
using System.Collections.Generic;
using FastTests.Graph;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            /*
                The AST for 

                with { from Movies where Name = “Star Wars Episode 1” } as lovedMovie
                with { from Movies } as recommendedMovie
                with edges(HasGenre) { order by Weight desc limit 1 } as dominantGenre
                match (lovedMovie)-[dominantGenre]->(Genre)<-[HasGenre(Weight > 0.8)]-(recommendedMovie)<-(u)
                select recommendedMovie           
                
             */

            //Console.WriteLine(graphQuery.ToString());

            using(var parsing = new Parsing())
            {
                parsing.CanParseComplexGraph();
            }

        }

        
    }
}
