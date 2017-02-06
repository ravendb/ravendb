using System.Linq;
using Raven.NewClient.Client.Indexes;

namespace SlowTests.Tests.Bugs.TransformResults
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

        }
    }

    public class QuestionWithVoteTotalTransformer : AbstractTransformerCreationTask<QuestionView>
    {
        public QuestionWithVoteTotalTransformer()
        {
            TransformResults = results =>
                                from result in results
                                let question = LoadDocument<Question>(result.QuestionId)
                                let user = LoadDocument<User>(question.UserId)
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
}
