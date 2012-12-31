using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.TransformResults
{
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

			Index(x => x.Content, FieldIndexing.Analyzed);
			Index(x => x.UserId, FieldIndexing.NotAnalyzed); // Case-sensitive searches

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

	public class Answers_ByAnswerEntity2 : AbstractIndexCreationTask<Answer2>
	{
		public Answers_ByAnswerEntity2()
		{
			Map = docs => from doc in docs
						  select new
						  {
							  AnswerId = doc.Id,
							  UserId = doc.UserId,
							  QuestionId = doc.QuestionId,
							  Content = doc.Content,
							  doc.Votes
						  };

			TransformResults = (database, results) =>
				from result in results
				let question = database.Load<Question2>(result.QuestionId.ToString())
				select new // AnswerEntity2
				{
					Id = result.Id,
					Question = question,
					Content = result.Content,
					UserId = result.UserId,
					Votes = from vote in result.Votes
							let answer = database.Load<Answer2>(vote.AnswerId.ToString())
							let firstVote = answer.Votes.FirstOrDefault(x => x.QuestionId == result.QuestionId)
							select new // AnswerVote2
									   {
										   vote.Id,
										   vote.Delta,
										   vote.QuestionId,
										   Answer = answer,
										   FirstVote = firstVote
									   }
				};
		}
	}

}