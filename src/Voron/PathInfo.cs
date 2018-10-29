using Sparrow.Utils;

namespace Voron
{
    public class DriveInfoByPath
    {
        public DriveInfoBase BasePath { get; set; }

        public DriveInfoBase JournalPath { get; set; }

        public DriveInfoBase TempPath { get; set; }
    }
}
