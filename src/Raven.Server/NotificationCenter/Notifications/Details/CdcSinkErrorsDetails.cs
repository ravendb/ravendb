using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class CdcSinkErrorsDetails : INotificationDetails
    {
        public const int MaxNumberOfErrors = 500;

        public Queue<CdcSinkErrorInfo> Errors { get; set; }

        public CdcSinkErrorsDetails()
        {
            Errors = new Queue<CdcSinkErrorInfo>();
        }

        public void Add(CdcSinkErrorInfo error)
        {
            Errors.Enqueue(error);

            if (Errors.Count > MaxNumberOfErrors)
                Errors.TryDequeue(out _);
        }

        public void Update(Queue<CdcSinkErrorInfo> errors)
        {
            var local = new Queue<CdcSinkErrorInfo>();

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
