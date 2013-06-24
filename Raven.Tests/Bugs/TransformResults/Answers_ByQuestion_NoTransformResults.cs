using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.TransformResults
{
	public class Answers_ByQuestion_NoTransformResults : AbstractIndexCreationTask<AnswerVote, AnswerViewItem>
	{
        public Answers_ByQuestion_NoTransformResults()
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

            this.IndexSortOptions.Add(x => x.VoteTotal, Raven.Abstractions.Indexing.SortOptions.Int);
		}
	}
}