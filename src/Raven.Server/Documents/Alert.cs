using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class Alert
    {
        /// <summary>
        /// Alert severity
        /// </summary>
        public AlertSeverity Severity { get; set; }

        /// <summary>
        /// Alert type
        /// </summary>
        public AlertType Type { get; set; }

        /// <summary>
        /// Key used to distinguish alerts with the same type
        /// </summary>
        public string Key { get; set; }


        /// <summary>
        /// Alert id
        /// </summary>
        public string Id => CreateId(Type, Key);

        /// <summary>
        /// Alert creation date.
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// <para>Purpose of this field is to avoid user from being flooded by recurring errors. We can display error i.e. once per day.</para>
        /// <para>This field might be used to dismiss alert until given date.</para>
        /// </summary>
        public DateTime? DismissedUntil { get; set; }

        /// <summary>
        /// Indicates if alert was read.
        /// </summary>
        public bool Read { get; set; }

        /// <summary>
        /// Alert message.
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Alert contents
        /// </summary>
        public IAlertContent Content { get; set; }

        public static string CreateId(AlertType type, string key)
        {
            return type + (key != null ? "/" + key : string.Empty);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Severity)] = Severity.ToString(),
                [nameof(Type)] = Type.ToString(),
                [nameof(Key)] = Key,
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(DismissedUntil)] = DismissedUntil,
                [nameof(Read)] = Read,
                [nameof(Message)] = Message,
                [nameof(Content)] = Content?.ToJson()
            };
        }
    }
}
