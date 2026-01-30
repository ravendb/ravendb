using System.Collections.Generic;
using System.Linq;

namespace Voron.Debugging
{
    public class FlatStorageReport
    {
        public DataFileReport DataFile { get; set; }
        public List<FlatTreeReport> Trees { get; set; }
        public List<FlatTableReport> Tables { get; set; }

        public static FlatStorageReport From(DetailedStorageReport report) =>
            new()
            {
                DataFile = report.DataFile,
                Trees = report.Trees?.Select(tree => new FlatTreeReport
                {
                    Name = tree.Name,
                    NumberOfEntries = tree.NumberOfEntries,
                    Depth = tree.Depth,
                    AllocatedSpaceInBytes = tree.AllocatedSpaceInBytes,
                    StreamReport = tree.Streams != null
                        ? new FlatStreamReport
                        {
                            NumberOfStreams = tree.Streams.NumberOfStreams,
                            TotalNumberOfAllocatedPages = tree.Streams.TotalNumberOfAllocatedPages,
                            AllocatedSpaceInBytes = tree.Streams.AllocatedSpaceInBytes
                        }
                        : null
                }).ToList(),
                Tables = report.Tables?.Select(table => new FlatTableReport
                {
                    Name = table.Name,
                    NumberOfEntries = table.NumberOfEntries,
                    DataSizeInBytes = table.DataSizeInBytes,
                    AllocatedSpaceInBytes = table.AllocatedSpaceInBytes,
                }).ToList()
            };
    }
    public class FlatTableReport
    {
        public string Name { get; set; }
        public long NumberOfEntries { get; set; }
        public long DataSizeInBytes { get; set; }
        public long AllocatedSpaceInBytes { get; set; }
    }
    public class FlatTreeReport
    {
        public string Name { get; set; }
        public long NumberOfEntries { get; set; }
        public int Depth { get; set; }
        public long AllocatedSpaceInBytes { get; set; }
        public FlatStreamReport StreamReport { get; set; }
    }

    public class FlatStreamReport
    {
        public long NumberOfStreams { get; set; }
        public long TotalNumberOfAllocatedPages { get; set; }
        public long AllocatedSpaceInBytes { get; set; }
    }
}
