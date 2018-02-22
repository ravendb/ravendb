using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class EtlErrorsDetails : INotificationDetails
    {
        public const int MaxNumberOfErrors = 500;

        public Queue<EtlErrorInfo> Errors { get; set; }

        public EtlErrorsDetails()
        {
            Errors = new Queue<EtlErrorInfo>();
        }

        public void Add(EtlErrorInfo error)
        {
            Errors.Enqueue(error);

            if (Errors.Count > MaxNumberOfErrors)
                Errors.TryDequeue(out _);
        }

        public void Update(Queue<EtlErrorInfo> errors)
        {
            var local = new Queue<EtlErrorInfo>();

            foreach (var existing in Errors)
            {
                var update = false;
                foreach (var newError in errors)
                {
                    if (existing.DocumentId == newError.DocumentId)
                    {
                        update = true;
                        break;
                    }
                }

                if (update)
                    continue;

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
