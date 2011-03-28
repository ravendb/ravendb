using System.Linq;
using Raven.Client.Indexes;

namespace LiveProjectionsBug
{
    public class QuestionWithVoteTotalIndex : AbstractIndexCreationTask<QuestionVote, QuestionView>
    {
        public QuestionWithVoteTotalIndex()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              QuestionId = doc.QuestionId,
                              VoteTotal = doc.Delta
                          };
            Reduce = mapped => from map in mapped
                               group map by map.QuestionId into g
                               select new
                               {
                                   QuestionId = g.Key,
                                   VoteTotal = g.Sum(x => x.VoteTotal)
                               };
            TransformResults = (database, results) =>
                                from result in results
                                let question = database.Load<Question>(result.QuestionId)
                                let user = database.Load<User>(question.UserId)
                                select new
                                {
                                    QuestionId = result.QuestionId,
                                    UserDisplayName = user.DisplayName,
                                    QuestionTitle = question.Title,
                                    QuestionContent = question.Content,
                                    VoteTotal = result.VoteTotal,
                                    User = user,
                                    Question = question
                                };

        }
    }
    public class Answers_ByQuestion : AbstractIndexCreationTask<AnswerVote, AnswerViewItem>
    {
        public Answers_ByQuestion()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              AnswerId = doc.AnswerId,
                              QuestionId = doc.QuestionId,
                              VoteTotal = doc.Delta
                          };

            Reduce = mapped => from map in mapped
                               group map by new
                               {
                                   map.QuestionId,
                                   map.AnswerId
                               } into g
                               select new
                               {
                                   AnswerId = g.Key.AnswerId,
                                   QuestionId = g.Key.QuestionId,
                                   VoteTotal = g.Sum(x => x.VoteTotal)
                               };

            TransformResults = (database, results) =>
                from result in results
                let answer = database.Load<Answer>(result.AnswerId)
                let user = database.Load<User>(answer.UserId)
                select new
                {
                    QuestionId = result.QuestionId,
                    AnswerId = result.AnswerId,
                    Content = answer.Content,
                    UserId = answer.UserId,
                    UserDisplayName = user.DisplayName,
                    VoteTotal = result.VoteTotal
                };
            this.SortOptions.Add(x => x.VoteTotal, Raven.Database.Indexing.SortOptions.Int);
        }
    }

    public class Answers_ByAnswerEntity : AbstractIndexCreationTask<Answer>
    {
        public Answers_ByAnswerEntity()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              AnswerId = doc.Id,
                              UserId = doc.UserId,
                              QuestionId = doc.QuestionId,
                              Content = doc.Content
                          };

            TransformResults = (database, results) =>
                                                        from result in results
                                                        let question = database.Load<Question>(result.QuestionId)
                                                        select new // AnswerEntity
                                                        {
                                                            Id = result.Id,
                                                            Question = question,
                                                            Content = result.Content,
                                                            UserId = result.UserId
                                                        };
        }
    }
    public class Votes_ByAnswerEntity : AbstractIndexCreationTask<AnswerVote>
    {
        public Votes_ByAnswerEntity()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              AnswerId = doc.AnswerId,
                              QuestionId = doc.QuestionId,
                              VoteTotal = doc.Delta
                          };

            TransformResults = (database, results) =>
                               from result in results
                               // this won't work because 'AnswerEntity' is not the stored type, its 'Answer' type
                               let answer = database.Load<AnswerEntity>(result.AnswerId)
                               // Should be like this
                               //let answer =database.Load<Answer, Answers_ByAnswerEntity>(result.AnswerId)
                               //                 .As<AnswerEntity>();
                                                     select new // AnswerVoteEntity
                                                        {
                                                            QuestionId = result.QuestionId,
                                                            Answer = answer,
                                                            Delta = result.Delta,
                                                        };
        }
    }
}
