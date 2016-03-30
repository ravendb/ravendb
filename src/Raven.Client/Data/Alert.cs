using System;

namespace Raven.Abstractions.Data
{
    public class Alert
    {
        /// <summary>
        /// Alert title.
        /// </summary>
        public string Title { get; set; }

        /// <summary>
        /// Alert creation date.
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Indicates if alert was observed (read).
        /// </summary>
        public bool Observed { get; set; }

        /// <summary>
        /// <para>Purpose of this field is to avoid user from being flooded by recurring errors. We can display error i.e. once per day.</para>
        /// <para>This field might be used to determinate when user dismissed given alert for the last time.</para>
        /// </summary>
        public DateTime? LastDismissedAt { get; set; }

        /// <summary>
        /// Alert message.
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Alert severity level.
        /// </summary>
        public bool IsError { get; set; }

        /// <summary>
        /// Exception that occured.
        /// </summary>
        public string Exception { get; set; }

        /// <summary>
        /// Unique key for the alert.
        /// </summary>
        public string UniqueKey { get; set; }
    }
}
