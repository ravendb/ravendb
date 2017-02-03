using System.Linq;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexes;

namespace SlowTests.Tests.Bugs.TransformResults
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
                              VoteTotal = doc.Delta,
                              DecimalTotal = doc.DecimalValue
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
                                   VoteTotal = g.Sum(x => x.VoteTotal),
                                   DecimalTotal = g.Sum(x => x.DecimalTotal)
                               };

            IndexSortOptions.Add(x => x.VoteTotal, SortOptions.NumericDefault);
        }
    }

    public class Answers_ByQuestionTransformer : AbstractTransformerCreationTask<AnswerViewItem>
    {
        public Answers_ByQuestionTransformer()
        {
            TransformResults = results =>
                from result in results
                let answer = LoadDocument<Answer>(result.AnswerId)
                let user = LoadDocument<User>(answer.UserId)
                select new
                {
                    QuestionId = result.QuestionId,
                    AnswerId = result.AnswerId,
                    Content = answer.Content,
                    UserId = answer.UserId,
                    UserDisplayName = user.DisplayName,
                    VoteTotal = result.VoteTotal,
                    result.DecimalTotal
                };
        }
    }
}
