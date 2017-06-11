using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using FastTests.Server.Documents.Notifications;
using FastTests.Tasks;
using Raven.Server.Utils;
using Raven.Client.Documents;
using RachisTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Raven.Client.Documents.Linq;
namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Name;
        }

        public class BeerReviewComment
        {
            public string UserId { get; set; }
            public string Comment { get; set; }
            public DateTime Date { get; set; }
            public List<BeerReviewComment> Replies { get; set; }
        }

        public class BeerReview
        {
            public string AuthorId { get; set; }
            public string ReviewText { get; set; }
            public string ReviewedBeerId { get; set; }
            public DateTime PublishDate { get; set; }
            public List<BeerReviewComment> Comments { get; set; }
        }

        public class ReviewTextWithAllCommentTextIndex : AbstractIndexCreationTask<BeerReview>
        {
            public class Result
            {
                public string ReviewText { get; set; }
                public string[] Comments { get; set; }
            }

            public ReviewTextWithAllCommentTextIndex()
            {
                Map = reviews => from review in reviews
                    select new
                    {
                        review.ReviewText,
                        Comments = Recurse(review, x => x.Comments).Select(x => x.Comment)
                    };
            }
        }

        public class AllUsersWhoCommentedAReview : AbstractTransformerCreationTask<BeerReview>
        {
            public class Result
            {
                public string ReviewedBeerId { get; set; }
                public string[] UserIDs { get; set; }
            }

            public AllUsersWhoCommentedAReview()
            {
                TransformResults = reviews => from review in reviews
                    select new
                    {
                        review.ReviewedBeerId,
                        UserId = Recurse(review, x => x.Comments).Select(x => x.UserId)
                    };
            }
        }

        public class BeerType
        {
            public string Name { get; set; }
            public string BreweryId { get; set; }
        }

        public class BrewerType
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
        }

        public class BeerAndBreweryTransformer : AbstractTransformerCreationTask<BeerType>
        {
            public class Result
            {
                public string Name { get; set; }
                public string BreweryName { get; set; }
                public string BreweryAddress { get; set; }
            }

            public BeerAndBreweryTransformer()
            {
                TransformResults = results => from result in results
                    let brewery = LoadDocument<BrewerType>(result.BreweryId)
                    select new
                    {
                        result.Name,
                        BreweryName = brewery.Name,
                        BreweryAddress = brewery.Address
                    };
            }
        }

        public class BeerByNameAndByBreweryNameIndex : AbstractIndexCreationTask<BeerType>
        {
            public class Result
            {
                public string Name { get; set; }
                public string BreweryName { get; set; }
            }

            public BeerByNameAndByBreweryNameIndex()
            {
                Map = beers => from beer in beers
                    let brewery = LoadDocument<BrewerType>(beer.BreweryId)
                    select new
                    {
                        beer.Name,
                        BreweryName = brewery.Name
                    };
            }
        }

        public class DocumentStoreHolder
        {
            private static readonly Lazy<IDocumentStore> store = 
                new Lazy<IDocumentStore>(CreateStore);

            public static IDocumentStore Store => store.Value;
            private static IDocumentStore CreateStore()
            {

                return new DocumentStore
                {
                    Urls = new[] { "http://localhost:8080" },
                    Database = "Northwind"
                }.Initialize();
            }
        }

        public static void Main(string[] args)
        {
            //using (var session = DocumentStoreHolder.Store.OpenSession())
            //{
            //    session.Store(new User {Name = "John Doe"});
            //    session.SaveChanges();
            //}


            //using (var session = DocumentStoreHolder.Store.OpenSession())
            //{
            //    var users = Queryable.Where(session.Query<User>(), u => u.Name.StartsWith("Jo"))
            //        .ToList();

            //    #region Actually Use Query Results here
            //    foreach (var user in users)
            //    {
            //        Console.WriteLine(user.Name);    
            //    }
            //    #endregion
            //}

            //new BeerAndBreweryTransformer().Execute(DocumentStoreHolder.Store);

            //using (var session = DocumentStoreHolder.Store.OpenSession())
            //{
            //    var beers = Queryable.Where(session.Query<BeerType>()
            //            .TransformWith<BeerAndBreweryTransformer, BeerAndBreweryTransformer.Result>(), b => b.Name.StartsWith("M"))
            //        .ToList();

            //    #region Actually Use Query Results here
            //    foreach (var user in beers)
            //    {
            //        Console.WriteLine(user.Name);
            //    }
            //    #endregion
            //}

            //using (var session = DocumentStoreHolder.Store.OpenSession())
            //{
            //    var beers = Queryable.Where(session.Query<BeerType>()
            //            .Include(b => b.BreweryId), b => b.Name.StartsWith("M"))
            //        .ToList();

            //    var brewery = session.Load<BrewerType>(beers[0].BreweryId);

            //    #region Actually Use Query Results here

            //    Console.WriteLine(brewery);
            //    foreach (var b in beers)
            //    {
            //        Console.WriteLine(b.Name);
            //    }
            //    #endregion
            //}

            //new BeerByNameAndByBreweryNameIndex().Execute(DocumentStoreHolder.Store);
            //using (var session = DocumentStoreHolder.Store.OpenSession())
            //{
            //    var beers = Queryable.Where(session.Query<BeerByNameAndByBreweryNameIndex.Result, BeerByNameAndByBreweryNameIndex>(), b => b.Name.StartsWith("M") &&
            //                    b.BreweryName.EndsWith("R"))
            //        .ToList();

            //    #region Actually Use Query Results here

            //    foreach (var b in beers)
            //    {
            //        Console.WriteLine(b.Name);
            //    }
            //    #endregion
            //}


            //using (var session = DocumentStoreHolder.Store.OpenSession())
            //{
            //    var reviews = session.Query<ReviewTextWithAllCommentTextIndex.Result, ReviewTextWithAllCommentTextIndex>()
            //                         .Where(review => review.ReviewText.Contains("Pilsner") &&
            //                                          review.Comments.ContainsAny(new []{ "Awesome", "Tasty", "Nice" }))
            //                         .ToList();

            //    #region Actually Use Query Results here


            //    foreach (var b in reviews)
            //    {
            //        Console.WriteLine(b.ReviewText);
            //    }
            //    #endregion

            //}


            for (var i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new RavenDB_7059())
                {
                    test.Cluster_identity_should_work_with_smuggler().Wait();
                }
            }
        }
    }


}
