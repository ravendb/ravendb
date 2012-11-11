using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.TransformResults
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