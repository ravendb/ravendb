using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class QueueSinkErrorsDetails : INotificationDetails
    {
        public const int MaxNumberOfErrors = 500;

        
        public Queue<QueueSinkErrorInfo> Errors { get; set; }

        public QueueSinkErrorsDetails()
        {
            Errors = new Queue<QueueSinkErrorInfo>();
        }

        public void Add(QueueSinkErrorInfo error)
        {
            Errors.Enqueue(error);

            if (Errors.Count > MaxNumberOfErrors)
                Errors.TryDequeue(out _);
        }

        public void Update(Queue<QueueSinkErrorInfo> errors)
        {
            var local = new Queue<QueueSinkErrorInfo>();

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
