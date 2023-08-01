using Sparrow.Server.Utils;

namespace Voron
{
    public sealed class DriveInfoByPath
    {
        public DriveInfoBase BasePath { get; set; }

        public DriveInfoBase JournalPath { get; set; }

        public DriveInfoBase TempPath { get; set; }
    }
}
