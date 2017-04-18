namespace Raven.Server.Documents.Studio
{
    public class FeedbackForm
    {
        public string Name { get; set; }
        public string Email { get; set; }
        public string Message { get; set; }
        public string StudioVersion { get; set; }
        public string ServerVersion { get; set; }
        public string UserAgent { get; set; }
    }
}