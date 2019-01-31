using System;
using Voron.Impl.Journal;

namespace Voron.Exceptions
{
    public class InvalidJournalException : Exception
    {
        public long Number { get; }

        public InvalidJournalException(string message) : base(message)
        {

        }

        public InvalidJournalException(string message, JournalInfo journalInfo) : base($"{message}. Journal details: " +
                                                                                                 $"{nameof(journalInfo.CurrentJournal)} - {journalInfo.CurrentJournal}, " +
                                                                                                 $"{nameof(journalInfo.LastSyncedJournal)} - {journalInfo.LastSyncedJournal}, " +
                                                                                                 $"{nameof(journalInfo.LastSyncedTransactionId)} - {journalInfo.LastSyncedTransactionId}, " +
                                                                                                 $"{nameof(journalInfo.Flags)} - {journalInfo.Flags}")
        {
            
        }

        public InvalidJournalException(long number, string path, JournalInfo journalInfo) : base($"No such journal '{path}'. Journal details: " +
                                                                                                 $"{nameof(journalInfo.CurrentJournal)} - {journalInfo.CurrentJournal}, " +
                                                                                                 $"{nameof(journalInfo.LastSyncedJournal)} - {journalInfo.LastSyncedJournal}, " +
                                                                                                 $"{nameof(journalInfo.LastSyncedTransactionId)} - {journalInfo.LastSyncedTransactionId}, " +
                                                                                                 $"{nameof(journalInfo.Flags)} - {journalInfo.Flags}")
        {
            Number = number;
        }

        public InvalidJournalException(long number, JournalInfo journalInfo) : base($"No such journal '{number}'. Journal details: " +
                                                                                    $"{nameof(journalInfo.CurrentJournal)} - {journalInfo.CurrentJournal}, " +
                                                                                    $"{nameof(journalInfo.LastSyncedJournal)} - {journalInfo.LastSyncedJournal}, " +
                                                                                    $"{nameof(journalInfo.LastSyncedTransactionId)} - {journalInfo.LastSyncedTransactionId}, " +
                                                                                    $"{nameof(journalInfo.Flags)} - {journalInfo.Flags}")
        {
            Number = number;
        }
    }
}
