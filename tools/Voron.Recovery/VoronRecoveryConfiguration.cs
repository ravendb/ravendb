﻿using Sparrow.Logging;

namespace Voron.Recovery
{
    public class VoronRecoveryConfiguration
    {
        public string PathToDataFile { get; set; }
        public string DataFileDirectory { get; set; }
        public string OutputFileName { get; set; }
        public int PageSizeInKB { get; set; } = 8;
        public int InitialContextSizeInMB { get; set; } = 1;
        public int InitialContextLongLivedSizeInKB { get; set; } = 16;
        public int ProgressIntervalInSec { get; set; } = 5;
        public bool DisableCopyOnWriteMode { get; set; }
        public bool? IgnoreInvalidJournalErrors { get; set; }
        public bool IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions { get; set; }
        public LogLevel LoggingLevel { get; set; } = LogLevel.Info;

        public byte[] MasterKey { get; set; }
        public bool IgnoreInvalidPagesInARow { get; set; } 
    }
}
