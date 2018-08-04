using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
        public static async Task Main(string[] args)
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

            using(var parsing = new FastTests.Graph.Parsing())
            {
                 parsing.CanRoundTripQueries(@"
with { from Movies where Genre = $genre } as m
match (u:Users)<-[r:Rated]-(m) and not (actor:Actors)-[:ActedOn]->(m) or (u)-[:Likes]->(actor)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH {
    FROM Actors
} AS actor
WITH EDGES(Rated) AS r
WITH EDGES(ActedOn) AS __alias1
WITH EDGES(Likes) AS __alias2
MATCH (((u)<-[r]-(m) AND NOT ((actor)-[__alias1]->(m))) OR (u)-[__alias2]->(actor))");
            }

        }

        
    }
}
