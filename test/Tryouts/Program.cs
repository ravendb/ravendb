using System.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Voron;
using Voron.Impl;
using Directory = System.IO.Directory;

// ReSharper disable InconsistentNaming

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Voron.Journal.EdgeCases())
                {
                    a.TransactionCommitShouldSetCurrentLogFileToNullIfItIsFull();
                }
            }

            if (DateTime.Now.Ticks > 1)
                return;

            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "stackoverflow"
            }.Initialize())
            {
                new Users_Registrations_ByMonth().Execute(store);
                new Users_Search().Execute(store);
                new Questions_Search().Execute(store);
                new Questions_Tags().Execute(store);
                new Questions_Tags_ByMonths().Execute(store);
                new Activity_ByMonth().Execute(store);

                var sp = Stopwatch.StartNew();
                var done = new HashSet<string>();
                while (true)
                {
                    bool hasStale = false;
                    DatabaseStatistics databaseStatistics;
                    try
                    {

                        databaseStatistics = store.DatabaseCommands.GetStatistics();
                    }
                    catch (Exception e)
                    {
                        var sb = new StringBuilder();
                        do
                        {
                            sb.Append("-->").Append(e.Message);
                            e = e.InnerException;
                        } while (e != null);
                        Console.WriteLine(sb.ToString());
                        continue;
                    }
                    foreach (var index in databaseStatistics.Indexes)
                    {
                        if (index.IsStale == false)
                        {
                            if (done.Add(index.Name))
                                Console.WriteLine(sp.Elapsed + " - " + index.Name);
                        }
                        else
                        {
                            hasStale = true;
                        }
                    }
                    if (hasStale == false)
                        break;
                    Thread.Sleep(250);
                }

                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine(sp.Elapsed + " all done! :-)");
            }

        }
    }

    public class Users_Search : AbstractIndexCreationTask<User>
    {
        public Users_Search()
        {
            Map = users => from user in users
                           select new
                           {
                               user.DisplayName,
                               user.LastAccessDate
                           };

            Index(user => user.DisplayName, FieldIndexing.Analyzed);
        }
    }


    public class Users_Registrations_ByMonth : AbstractIndexCreationTask<User, Users_Registrations_ByMonth.Result>
    {
        public class Result
        {
            public string Month;
            public int Count;
        }

        public Users_Registrations_ByMonth()
        {
            Map = users => from user in users
                           select new
                           {
                               Count = 1,
                               Month = user.CreationDate.ToString("yyyy-MM")
                           };

            Reduce = results => from result in results
                                group result by result.Month
                                into g
                                select new
                                {
                                    Month = g.Key,
                                    Count = g.Sum(x => x.Count)
                                };
        }
    }


    public class User
    {
        public int Reputation { get; set; }
        public DateTimeOffset CreationDate { get; set; }
        public string DisplayName { get; set; }
        public DateTimeOffset LastAccessDate { get; set; }
        public int Views { get; set; }
        public int UpVotes { get; set; }
        public int DownVotes { get; set; }
        public Uri ProfileImageUrl { get; set; }
        public int AccountId { get; set; }
    }

    public class Questions_Tags : AbstractIndexCreationTask<Question, Questions_Tags.Result>
    {
        public class Result
        {
            public int Count;
            public int Answers;
            public int AcceptedAnswers;
            public string Tag;
        }

        public Questions_Tags()
        {
            Map = questions =>
                from q in questions
                from tag in q.Tags
                select new
                {
                    Tag = tag,
                    Count = 1,
                    Answers = q.AnswerCount,
                    AcceptedAnswers = q.AcceptedAnswerId != null ? 1 : 0
                };

            Reduce = results =>
                from result in results
                group result by result.Tag into g
                select new
                {
                    Tag = g.Key,
                    Count = g.Sum(x => x.Count),
                    Answers = g.Sum(x => x.Answers),
                    AcceptedAnswers = g.Sum(x => x.AcceptedAnswers)
                };
        }
    }

    public class Questions_Search : AbstractIndexCreationTask<Question>
    {
        public Questions_Search()
        {
            Map = questions =>
                from q in questions
                select new
                {
                    q.CreationDate,
                    q.Tags,
                    q.Score,
                    q.Title,
                    Users = new object[]
                    {
                        q.OwnerUserId,
                        q.LastEditorUserId,
                        q.Answers.Select(x => x.OwnerUserId)
                    }
                };

            Index(x => x.Title, FieldIndexing.Analyzed);

        }
    }

    public class Questions_Tags_ByMonths : AbstractIndexCreationTask<Question, Questions_Tags_ByMonths.Result>
    {
        public class Result
        {
            public int Count;
            public string Tag;
            public string Month;
        }

        public Questions_Tags_ByMonths()
        {
            Map = questions =>
                from q in questions
                from tag in q.Tags
                select new
                {
                    Tag = tag,
                    Count = 1,
                    Month = q.CreationDate.ToString("yyyy-MM")
                };

            Reduce = results =>
                from result in results
                group result by new { result.Tag, result.Month } into g
                select new
                {
                    g.Key.Month,
                    g.Key.Tag,
                    Count = g.Sum(x => x.Count),
                };
        }
    }


    public class Activity_ByMonth : AbstractMultiMapIndexCreationTask<Activity_ByMonth.Result>
    {
        public class Result
        {
            public int Users;
            public string Month;
        }

        public Activity_ByMonth()
        {
            AddMap<Question>(questions =>
                from q in questions
                select new
                {
                    Month = q.CreationDate.ToString("yyyy-MM"),
                    Users = 1
                });

            AddMap<Question>(questions =>
               from q in questions
               from a in q.Answers
               group a by new // distinct users by month
               {
                   a.OwnerUserId,
                   Month = a.CreationDate.ToString("yyyy-MM")
               } into g
               select new
               {
                   g.Key.Month,
                   Users = g.Count()
               });

            Reduce = results =>
                from result in results
                group result by result.Month into g
                select new
                {
                    Month = g.Key,
                    Users = g.Sum(x => x.Users),
                };
        }
    }

    public class Answers
    {
        public int Id { get; set; }
        public DateTimeOffset CreationDate { get; set; }
        public int Score { get; set; }
        public string Body { get; set; }
        public int OwnerUserId { get; set; }
        public DateTimeOffset LastActivityDate { get; set; }
        public int CommentCount { get; set; }
    }

    public class Question
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
