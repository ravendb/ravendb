using System.Collections.Generic;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Debugging;

namespace Voron.Data.Tables
{
    public class TableReport
    {
        public TableReport(long ownedSizeInBytes, long usedSizeInBytes, bool calculateDensity)
        {
            OwnedSizeInBytes = DataSizeInBytes = ownedSizeInBytes;
            UsedSizeInBytes = usedSizeInBytes;

            if (calculateDensity == false)
                UsedSizeInBytes = -1;

            Indexes = new List<TreeReport>();
            Structure = new List<TreeReport>();
        }

        public void AddStructure(FixedSizeTree fst, bool calculateDensity)
        {
            var report = StorageReportGenerator.GetReport(fst, calculateDensity);
            AddStructure(report, fst.Llt.PageSize, calculateDensity);
        }

        public void AddStructure(Tree tree, bool calculateDensity)
        {
            var report = StorageReportGenerator.GetReport(tree, calculateDensity);
            AddStructure(report, tree.Llt.PageSize, calculateDensity);
        }

        private void AddStructure(TreeReport report, int pageSize, bool calculateDensity)
        {
            Structure.Add(report);

            var ownedSizeInBytes = report.PageCount * pageSize;
            OwnedSizeInBytes += ownedSizeInBytes;

            if (calculateDensity)
                UsedSizeInBytes += (long)(ownedSizeInBytes * report.Density);
        }

        public void AddIndex(FixedSizeTree fst, bool calculateDensity)
        {
            var report = StorageReportGenerator.GetReport(fst, calculateDensity);
            AddIndex(report, fst.Llt.PageSize, calculateDensity);
        }

        public void AddIndex(Tree tree, bool calculateDensity)
        {
            var report = StorageReportGenerator.GetReport(tree, calculateDensity);
            AddIndex(report, tree.Llt.PageSize, calculateDensity);
        }

        private void AddIndex(TreeReport report, int pageSize, bool calculateDensity)
        {
            Indexes.Add(report);

            var ownedSizeInBytes = report.PageCount * pageSize;
            OwnedSizeInBytes += ownedSizeInBytes;

            if (calculateDensity)
                UsedSizeInBytes += (long)(ownedSizeInBytes * report.Density);
        }

        public void AddData(RawDataSection section, bool calculateDensity)
        {
            var ownedSizeInBytes = section.Size;
            OwnedSizeInBytes += ownedSizeInBytes;
            DataSizeInBytes += ownedSizeInBytes;

            if (calculateDensity)
                UsedSizeInBytes += (long)(ownedSizeInBytes * section.Density);
        }

        public List<TreeReport> Structure { get; }
        public List<TreeReport> Indexes { get; }
        public string Name { get; set; }
        public long NumberOfEntries { get; set; }
        public long DataSizeInBytes { get; private set; }
        public long OwnedSizeInBytes { get; private set; }
        public long UsedSizeInBytes { get; private set; }
    }
}