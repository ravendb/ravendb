using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client.Client;
using Raven.Client.Indexes;

namespace LiveProjectionsBug
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Working ...");

            write_then_read_from_stack_over_flow_types();

            Console.WriteLine("Finished !");
            Console.ReadLine();

        }
        private static void write_then_read_from_stack_over_flow_types()
        {            
            EmbeddableDocumentStore documentStore = GetDocumentStore();
            IndexCreation.CreateIndexes(typeof(QuestionWithVoteTotalIndex).Assembly, documentStore);

            var questionId = @"question\259";
            using (var session = documentStore.OpenSession())
            {
                var user = new User();
                user.Id = @"user\222";
                user.DisplayName = "John Doe";
                session.Store(user);

                var question = new Question();
                question.Id = questionId;
                question.Title = "How to do this in RavenDb?";
                question.Content = "I'm trying to find how to model documents for better DDD support.";
                question.UserId = @"user\222";
                session.Store(question);

                var vote1 = new QuestionVote();
                vote1.QuestionId = questionId;
                vote1.Delta = 2;
                session.Store(vote1);
                var vote2 = new QuestionVote();
                vote2.QuestionId = questionId;
                vote2.Delta = 3;
                session.Store(vote2);

                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var questionInfo = session.Query<QuestionView, QuestionWithVoteTotalIndex>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                    .Where(x => x.QuestionId == questionId)
                    .SingleOrDefault();
                if (questionInfo == null)
                    Console.WriteLine(" question not found ");
                else
                {
                    Console.WriteLine("Question Title: {0}, votes: {1}",
                        questionInfo.QuestionTitle, questionInfo.VoteTotal);
                    Console.WriteLine("User name {0}", questionInfo.User.DisplayName);
                    Assert.Assert(!string.IsNullOrEmpty(questionInfo.User.DisplayName), "User.DisplayName was not loaded");
                    Console.WriteLine("Content: {0}", questionInfo.Question.Content);
                    Debug.Assert(!string.IsNullOrEmpty(questionInfo.Question.Content), "Question.Content was not loaded");
                }
            }
        }

      
    }
}
