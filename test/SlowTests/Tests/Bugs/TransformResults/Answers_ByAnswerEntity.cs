using System.Linq;
using Raven.Client.Documents.Indexes;

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

            Index(x => x.Content, FieldIndexing.Search);
            Index(x => x.UserId, FieldIndexing.Exact); // Case-sensitive searches
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
}
