using System.Linq;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexes;

namespace SlowTests.Tests.Bugs.TransformResults
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
        }
    }

    public class Answers_ByAnswerEntityTransformer : AbstractTransformerCreationTask<Answer>
    {
        public Answers_ByAnswerEntityTransformer()
        {
            TransformResults = results =>
                from result in results
                let question = LoadDocument<Question>(result.QuestionId)
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
        }
    }

    public class Answers_ByAnswerEntityTransformer2 : AbstractTransformerCreationTask<Answer2>
    {
        public Answers_ByAnswerEntityTransformer2()
        {
            TransformResults = results =>
                from result in results
                let question = LoadDocument<Question2>(result.QuestionId.ToString())
                select new // AnswerEntity2
                {
                    Id = result.Id,
                    Question = question,
                    Content = result.Content,
                    UserId = result.UserId,
                    Votes = from vote in result.Votes
                            let answer = LoadDocument<Answer2>(vote.AnswerId.ToString())
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
