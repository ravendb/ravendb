namespace Raven.Tests.Bugs.TransformResults
{
    public class AnswerVoteEntity
    {
        public string Id { get; set; }
        public string QuestionId { get; set; }
        public AnswerEntity Answer { get; set; }
        public int Delta { get; set; }
    }
}