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

        public DateTime Started { get; set; }
        public DateTime? Completed { get; set; }
        public bool IsRunning { get; set; }
        public List<BackupMessage> Messages { get; set; }

        public class BackupMessage
        {
            public string Message { get; set; }
            public DateTime Timestamp { get; set; }
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
                       Severity == item.Severity;
            }

            public override int GetHashCode()
            {
                return (Message != null ? Message.GetHashCode() : 0) ^ Timestamp.GetHashCode() ^
                       (Severity.GetHashCode() << 16);
            }
        }
    }
}