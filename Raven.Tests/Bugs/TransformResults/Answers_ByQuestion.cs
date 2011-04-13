using System.Linq;
using Raven.Client.Indexes;

namespace LiveProjectionsBug
{
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
            this.SortOptions.Add(x => x.VoteTotal, Raven.Abstractions.Indexing.SortOptions.Int);
        }
    }
}