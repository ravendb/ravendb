using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.Commercial
{
    public sealed class LeasedLicense
    {
        public License License { get; set; }

        public string Title { get; set; }

        public string Message { get; set; }

        public string ErrorMessage { get; set; }

        public NotificationSeverity NotificationSeverity { get; set; }
    }
}
