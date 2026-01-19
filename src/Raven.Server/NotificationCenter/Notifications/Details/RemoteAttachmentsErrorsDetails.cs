using System.Collections.Concurrent;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RemoteAttachmentsErrorsDetails : INotificationDetails
    {
        public const int MaxNumberOfErrors = 100;

        public ConcurrentQueue<RemoteAttachmentsErrorInfo> Errors { get; set; }

        public RemoteAttachmentsErrorsDetails()
        {
            Errors = new ConcurrentQueue<RemoteAttachmentsErrorInfo>();
        }

        public void Add(RemoteAttachmentsErrorInfo error)
        {
            Errors.Enqueue(error);

            if (Errors.Count > MaxNumberOfErrors)
                Errors.TryDequeue(out _);
        }

        public DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue();

            var errors = new DynamicJsonArray();

            foreach (var details in Errors)
            {
                errors.Add(details.ToJson());
            }

            result[nameof(Errors)] = errors;

            return result;
        }
    }
}
