using System.Linq;
using Raven.Client.Documents.Indexes;

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
}
