namespace LiveProjectionsBug
{
    public class User
    {
        public string Id { get; set; }
        public string DisplayName { get; set; }
    }

    public class Question
    {
        public string Id { get; set; }
        public string UserId { get; set; }
        public string Title { get; set; }
        public string Content { get; set; }
    }

    public class QuestionVote
    {
        public string QuestionId { get; set; }
        public int Delta { get; set; }
    }

    public class Answer
    {
        public string UserId { get; set; }
        public string QuestionId { get; set; }
        public string Content { get; set; }
    }

    public class AnswerVote
    {
        public string QuestionId { get; set; }
        public string AnswerId { get; set; }
        public int Delta { get; set; }
    }
}
