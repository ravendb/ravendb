namespace SlowTests.Core.Utils.Entities
{
    public class Post
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Desc { get; set; }
        public Post[] Comments { get; set; }
        public string[] AttachmentIds { get; set; }
    }
}