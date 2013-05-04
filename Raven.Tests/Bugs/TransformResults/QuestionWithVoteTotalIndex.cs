using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.TransformResults
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
}
