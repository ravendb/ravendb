package raven.tests.bugs.transformresults;

import raven.abstractions.indexing.FieldIndexing;
import raven.client.indexes.AbstractIndexCreationTask;


public class Answers_ByAnswerEntity extends AbstractIndexCreationTask {
  public Answers_ByAnswerEntity() {
    map = "from doc in docs.Answers select new { AnswerId = doc.Id, UserId = doc.UserId, QuestionId = doc.QuestionId, Content = doc.Content }";

    QAnswer x = QAnswer.answer;
    index(x.content, FieldIndexing.ANALYZED);
    index(x.userId, FieldIndexing.NOT_ANALYZED); // case-sensitive searches

    transformResults = "results.Select( result => new { " +
    		"result = result ," +
    		"question = Database.Load(result.QuestionId)}).Select(this0 => new {" +
    		"Id = this0.result.__document_id," +
    		"Question = this0.question," +
    		"Content = this0.result.Content," +
    		"UserId = this0.result.UserId})";
  }
}
