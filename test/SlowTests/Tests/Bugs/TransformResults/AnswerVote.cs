namespace SlowTests.Tests.Bugs.TransformResults
{
    public class AnswerVote
    {
        public string QuestionId { get; set; }
        public string AnswerId { get; set; }
        public int Delta { get; set; }
        public decimal DecimalValue { get; set; }
    }

    public class AnswerVote2
    {
        public string Id { get; set; }
        public string QuestionId { get; set; }
        public string AnswerId { get; set; }
        public int Delta { get; set; }
    }
}
