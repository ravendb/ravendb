using System.Linq;
using Raven.Client.Indexes;

namespace LiveProjectionsBug
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
}