namespace LiveProjectionsBug
{
    public class AnswerEntity
    {
        public string Id { get; set; }
        public string UserId { get; set; }
        public Question Question { get; set; }
        public string Content { get; set; }
    }
}