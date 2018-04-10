using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Smuggler
{
    internal class PeriodicBackupFileExtensionComparer : IComparer<string>
    {
        public static PeriodicBackupFileExtensionComparer Instance = new PeriodicBackupFileExtensionComparer();

        private PeriodicBackupFileExtensionComparer()
        {
                
        }

        public int Compare(string x, string y)
        {
            if (string.Equals(x, y))
                return 0;

            if (string.Equals(x, Constants.Documents.PeriodicBackup.SnapshotExtension, StringComparison.OrdinalIgnoreCase))
                return -1;

            if (string.Equals(x, Constants.Documents.PeriodicBackup.FullBackupExtension, StringComparison.OrdinalIgnoreCase))
                return -1;

            return 1;
        }
    }
}
