namespace SlowTests.Tests.Bugs.TransformResults
{
    public class AnswerEntity
    {
        public string Id { get; set; }
        public string UserId { get; set; }
        public Question Question { get; set; }
        public string Content { get; set; }
    }
    public class AnswerEntity2
    {
        public string Id { get; set; }
        public string UserId { get; set; }
        public Question2 Question { get; set; }
        public string Content { get; set; }
        public AnswerVoteEntity2[] Votes { get; set; }

    }
}
