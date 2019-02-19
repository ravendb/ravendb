using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7078 : RavenTestBase
    {
        [Fact]
        public void Invalid_map_function_due_to_raven_linq_optimizer_not_supporting_select_into()
        {
            using (var store = GetDocumentStore())
            {
                new AnswersActivity_ByMonth().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Question()
                    {
                        Answers = new Answers[1]
                        {
                            new Answers()
                            {
                                OwnerUserId = 1,
                                CreationDate = SystemTime.UtcNow
                            }
                        },
                        CreationDate = SystemTime.UtcNow,
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<AnswersActivity_ByMonth.Result, AnswersActivity_ByMonth>().ToList();

                    Assert.Equal(1, results[0].Users);
                }
            }
        }

        public class AnswersActivity_ByMonth : AbstractMultiMapIndexCreationTask<AnswersActivity_ByMonth.Result>
        {
            public class Result
            {
                public int Users;
                public string Month;
            }

            public override string IndexName => "AnswersActivity/ByMonth";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition()
                {
                    Maps =
                    {
                        @"from q in docs.Questions
                    from a in q.Answers
                    select new
                    {
                        q = q,
                        a = a
                    } into this0
                    group this0.a by new
                    {
                        OwnerUserId = this0.a.OwnerUserId,
                        Month = this0.a.CreationDate.ToString(""yyyy-MM"")
                    } into g
                    select new
                    {
                        Month = g.Key.Month,
                        Users = Enumerable.Count(g)
                    }"
                    },
                    Reduce = @"from result in results
                    group result by result.Month into g
                    select new
                    {
                        Month = g.Key,
                        Users = g.Sum(x => x.Users),
                    }"
                };
            }
        }

        private class Answers
        {
            public int Id { get; set; }
            public DateTimeOffset CreationDate { get; set; }
            public int Score { get; set; }
            public string Body { get; set; }
            public int OwnerUserId { get; set; }
            public DateTimeOffset LastActivityDate { get; set; }
            public int CommentCount { get; set; }
        }

        private class Question
        {
            public Answers[] Answers { get; set; }
            public int? AcceptedAnswerId { get; set; }
            public DateTimeOffset CreationDate { get; set; }
            public int Score { get; set; }
            public int ViewCount { get; set; }
            public string Body { get; set; }
            public int OwnerUserId { get; set; }
            public int LastEditorUserId { get; set; }
            public DateTimeOffset LastEditDate { get; set; }
            public DateTimeOffset LastActivityDate { get; set; }
            public string Title { get; set; }
            public string[] Tags { get; set; }
            public int AnswerCount { get; set; }
            public int CommentCount { get; set; }
            public int FavoriteCount { get; set; }
        }
    }
}