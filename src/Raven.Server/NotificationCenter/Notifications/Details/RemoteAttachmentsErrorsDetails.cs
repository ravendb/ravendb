using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RemoteAttachmentsErrorsDetails : INotificationDetails
    {
        public const int MaxNumberOfErrors = 100;

        public Queue<RemoteAttachmentsErrorInfo> Errors { get; set; }

        public RemoteAttachmentsErrorsDetails()
        {
            Errors = new Queue<RemoteAttachmentsErrorInfo>();
        }

        public void Add(RemoteAttachmentsErrorInfo error)
        {
            Errors.Enqueue(error);

            if (Errors.Count > MaxNumberOfErrors)
                Errors.TryDequeue(out _);
        }

        public void Update(List<RemoteAttachmentsErrorInfo> errors)
        {
            var local = new Queue<RemoteAttachmentsErrorInfo>();

            foreach (var existing in Errors)
            {
                local.Enqueue(existing);
            }

            foreach (var newError in errors)
            {
                local.Enqueue(newError);
            }

            Errors = local;

            while (Errors.Count > MaxNumberOfErrors)
            {
                Errors.TryDequeue(out _);
            }
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
