namespace SlowTests.Tests.Bugs.TransformResults
{
    public class AnswerVoteEntity
    {
        public string Id { get; set; }
        public string QuestionId { get; set; }
        public AnswerEntity Answer { get; set; }
        public int Delta { get; set; }
    }

    public class AnswerVoteEntity2
    {
        public string Id { get; set; }
        public string QuestionId { get; set; }
        public AnswerEntity2 Answer { get; set; }
        public int Delta { get; set; }
    }
}
