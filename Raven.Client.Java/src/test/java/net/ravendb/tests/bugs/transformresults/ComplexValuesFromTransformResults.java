package net.ravendb.tests.bugs.transformresults;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;

public class ComplexValuesFromTransformResults extends RemoteClientTest {
//FIXME: finish me



  public static String createEntities(IDocumentStore documentStore) throws Exception {
    final String questionId = "question/259";
    final String answerId = "answer/540";

    try (IDocumentSession session = documentStore.openSession()) {
      User user = new User("user/222", "John Doe");
      session.store(user);

      Question question = new Question();
      question.setId(questionId);
      question.setTitle("How to do this in RavenDb?");
      question.setContent("I'm trying to find how to model documents for better DDD support.");
      question.setUserId("user/222");
      session.store(question);

      AnswerEntity answer = new AnswerEntity();
      answer.setId(answerId);
      answer.setQuestion(question);
      answer.setContent("This is doable");
      answer.setUserId(user.getId());

      Answer a = new Answer();
      a.setId(answer.getId());
      a.setUserId(answer.getUserId());
      a.setQuestionId(answer.getQuestion().getId());
      a.setContent(answer.getContent());
      session.store(a);

      AnswerVoteEntity vote1 = new AnswerVoteEntity();
      vote1.setId("votes\\1");
      vote1.setAnswer(answer);
      vote1.setQuestionId(questionId);
      vote1.setDelta(2);

      AnswerVote answerVote = new AnswerVote();
      answerVote.setQuestionId(vote1.getQuestionId());
      answerVote.setAnswerId(vote1.getAnswer().getId());
      answerVote.setDelta(vote1.getDelta());

      session.store(answerVote);

      AnswerVoteEntity vote2 = new AnswerVoteEntity();
      vote2.setId("votes\\2");
      vote2.setAnswer(answer);
      vote2.setQuestionId(questionId);
      vote2.setDelta(3);
      AnswerVote answerVote2 = new AnswerVote();
      answerVote2.setQuestionId(vote2.getQuestionId());
      answerVote2.setAnswerId(vote2.getAnswer().getId());
      answerVote2.setDelta(vote2.getDelta());
      session.store(answerVote2);

      session.saveChanges();
    }
    return answerId;
  }
}
