namespace Raven.Studio.Messages
{
    using Features.Indexes;

    public class IndexUpdated : NotificationRaised
    {
        public IndexUpdated() : base("Index updated", NotificationLevel.Info)
        {
        }

        public EditIndexViewModel Index { get; set; }

        public bool IsRemoved { get; set; }
    }
}