using System.Linq;
using Raven.NewClient.Client.Indexes;

namespace SlowTests.Tests.Bugs.TransformResults
{
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
        }
    }

    public class Votes_ByAnswerEntityTransfotmer : AbstractTransformerCreationTask<AnswerVote>
    {
        public Votes_ByAnswerEntityTransfotmer()
        {
            TransformResults = results =>
                from result in results
                // this won't work because 'AnswerEntity' is not the stored type, its 'Answer' type
                let answer = LoadDocument<AnswerEntity>(result.AnswerId)
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
