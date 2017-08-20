using System.Linq;
using Raven.Client.Documents.Indexes;

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
}
