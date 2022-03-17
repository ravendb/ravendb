using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class RavenDb1192_MapReduceNestedItemsTests : RavenTestBase
    {
        public RavenDb1192_MapReduceNestedItemsTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Reduce_Nested_List_Of_KeyValuePair_Objects()
        {
            DoTest<TestIndex1, TestIndex1.Result>();
        }

        [Fact]
        public void Can_Reduce_Nested_Dictionaries()
        {
            DoTest<TestIndex2, TestIndex2.Result>();
        }

        private void DoTest<TIndex, TResult>()
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TIndex());

                using (var session = store.OpenSession())
                {
                    #region Adding Sample Data

                    session.Store(new Submission
                    {
                        PersonName = "Alice",
                        SurveyId = "surveys/1",
                        Answers = new[]
                        {
                            new Answer
                            {
                                QuestionId = "questions/1",
                                Responses = new[] {"b", "d"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/2",
                                Responses = new[] {"b"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/3",
                                Responses = new[] {"a", "b", "d"}
                            }
                        }
                    });

                    session.Store(new Submission
                    {
                        PersonName = "Bob",
                        SurveyId = "surveys/1",
                        Answers = new[]
                        {
                            new Answer
                            {
                                QuestionId = "questions/1",
                                Responses = new[] {"b", "c"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/2",
                                Responses = new[] {"a", "b", "d"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/3",
                                Responses = new[] {"c"}
                            }
                        }
                    });

                    session.Store(new Submission
                    {
                        PersonName = "Charlie",
                        SurveyId = "surveys/2",
                        Answers = new[]
                        {
                            new Answer
                            {
                                QuestionId = "questions/4",
                                Responses = new[] {"a", "b", "e"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/5",
                                Responses = new[] {"b", "c", "d"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/6",
                                Responses = new[] {"a", "c", "e"}
                            }
                        }
                    });

                    session.Store(new Submission
                    {
                        PersonName = "David",
                        SurveyId = "surveys/2",
                        Answers = new[]
                        {
                            new Answer
                            {
                                QuestionId = "questions/4",
                                Responses = new[] {"d", "e"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/5",
                                Responses = new[] {"a", "b", "d"}
                            },
                            new Answer
                            {
                                QuestionId = "questions/6",
                                Responses = new[] {"c"}
                            }
                        }
                    });

                    #endregion

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TResult, TIndex>().ToList();

                    // Just testing that some data returns.
                    Assert.Equal(6, results.Count);

                    // (The main thing is that the index builds and the query can execute without throwing an exception.)
                }
            }
        }

        private class Submission
        {
            public string PersonName { get; set; }
            public string SurveyId { get; set; }
            public IList<Answer> Answers { get; set; }
        }

        private class Answer
        {
            public string QuestionId { get; set; }
            public IList<string> Responses { get; set; } // multiple responses are allowed
        }

        private class TestIndex1 : AbstractIndexCreationTask<Submission, TestIndex1.Result>
        {
            // This index works, using a list of KeyValuePair objects.
            // It would also work using any list of any custom or anonymous type.

            public class Result
            {
                public string SurveyId { get; set; }
                public string QuestionId { get; set; }
                public int AnswerCount { get; set; }
                public IList<KeyValuePair<string, int>> ResponseCounts { get; set; }
            }

            public TestIndex1()
            {
                Map = submissions => from submission in submissions
                                     from answer in submission.Answers
                                     select new
                                     {
                                         submission.SurveyId,
                                         answer.QuestionId,
                                         AnswerCount = 1,
                                         ResponseCounts = answer.Responses.Select(x => new KeyValuePair<string, int>(x, 1))
                                     };

                Reduce = results => from result in results
                                    group result by new { result.SurveyId, result.QuestionId }
                                        into g
                                    select new
                                    {
                                        g.Key.SurveyId,
                                        g.Key.QuestionId,
                                        AnswerCount = g.Sum(x => x.AnswerCount),
                                        ResponseCounts = g.SelectMany(x => x.ResponseCounts)
                                                          .GroupBy(x => x.Key)
                                                          .OrderBy(x => x.Key)
                                                          .Select(x => new KeyValuePair<string, int>(x.Key, x.Sum(y => y.Value)))
                                    };
            }
        }

        private class TestIndex2 : AbstractIndexCreationTask<Submission, TestIndex1.Result>
        {
            // This index doesn't work.  It is identical to the first index, but uses a dictionary.
            // The use of .ToDictionary() appears to be the problem.  It throws an exception with the following message:
            //
            //      Cannot use a lambda expression as an argument to a dynamically dispatched operation without
            //      first casting it to a delegate or expression tree type
            //

            public class Result
            {
                public string SurveyId { get; set; }
                public string QuestionId { get; set; }
                public int AnswerCount { get; set; }
                public Dictionary<string, int> ResponseCounts { get; set; }
            }

            public TestIndex2()
            {
                Map = submissions => from submission in submissions
                                     from answer in submission.Answers
                                     select new
                                     {
                                         submission.SurveyId,
                                         answer.QuestionId,
                                         AnswerCount = 1,
                                         ResponseCounts = answer.Responses.ToDictionary(x => x, x => 1)
                                     };

                Reduce = results => from result in results
                                    group result by new { result.SurveyId, result.QuestionId }
                                        into g
                                    select new
                                    {
                                        g.Key.SurveyId,
                                        g.Key.QuestionId,
                                        AnswerCount = g.Sum(x => x.AnswerCount),
                                        ResponseCounts = g.SelectMany(x => x.ResponseCounts)
                                                          .GroupBy(x => x.Key)
                                                          .OrderBy(x => x.Key)
                                                          .ToDictionary(x => x.Key, x => x.Sum(y => y.Value))
                                    };
            }
        }
    }
}
