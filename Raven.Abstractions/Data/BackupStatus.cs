//-----------------------------------------------------------------------
// <copyright file="BackupStatus.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class BackupStatus
    {
        public enum BackupMessageSeverity
        {
            Informational,
            Error
        }

        public const string RavenBackupStatusDocumentKey = "Raven/Backup/Status";

        public BackupStatus()
        {
            Messages = new List<BackupMessage>();
        }

        /// <summary>
        /// Backup start time.
        /// </summary>
        public DateTime Started { get; set; }

        /// <summary>
        /// Backup completed time.
        /// </summary>
        public DateTime? Completed { get; set; }

        /// <summary>
        /// Indicates if backup is currently running.
        /// </summary>
        public bool IsRunning { get; set; }

        /// <summary>
        /// List of backup messages.
        /// </summary>
        public List<BackupMessage> Messages { get; set; }

        public class BackupMessage
        {
            /// <summary>
            /// Message text.
            /// </summary>
            public string Message { get; set; }

            /// <summary>
            /// Message details.
            /// </summary>
            public string Details { get; set; }

            /// <summary>
            /// Created timestamp.
            /// </summary>
            public DateTime Timestamp { get; set; }

            /// <summary>
            /// Message severity.
            /// </summary>
            public BackupMessageSeverity Severity { get; set; }

            public override bool Equals(object obj)
            {
                var item = obj as BackupMessage;

                if (item == null)
                {
                    return false;
                }

                return Message == item.Message &&
                       Timestamp == item.Timestamp &&
                       Details == item.Details &&
                       Severity == item.Severity;
            }

            public override int GetHashCode()
            {
                return (Message != null ? Message.GetHashCode() : 0) ^ (Details != null ? Details.GetHashCode() : 1) ^ Timestamp.GetHashCode() ^
                       (Severity.GetHashCode() << 16);
            }
        }
    }
}
