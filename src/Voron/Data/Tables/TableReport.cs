using System;
using System.Collections.Generic;
using System.Threading;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Debugging;
using Voron.Global;

namespace Voron.Data.Tables
{
    public class TableReport
    {
        public TableReport(long allocatedSpaceInBytes, long usedSizeInBytes, bool calculateExactSizes)
        {
            AllocatedSpaceInBytes = DataSizeInBytes = allocatedSpaceInBytes;
            UsedSizeInBytes = usedSizeInBytes;

            if (calculateExactSizes == false)
                UsedSizeInBytes = -1;

            Indexes = new List<TreeReport>();
            Structure = new List<TreeReport>();
        }

        public void AddStructure(FixedSizeTree fst, bool includeDetails)
        {
            var report = StorageReportGenerator.GetReport(fst, includeDetails);
            AddStructure(report, Constants.Storage.PageSize, includeDetails);
        }

        public void AddStructure(Tree tree, bool includeDetails)
        {
            var report = StorageReportGenerator.GetReport(tree, includeDetails);
            AddStructure(report, Constants.Storage.PageSize, includeDetails);
        }

        private void AddStructure(TreeReport report, int pageSize, bool includeDetails)
        {
            Structure.Add(report);

            var allocatedSpaceInBytes = report.PageCount * pageSize;
            AllocatedSpaceInBytes += allocatedSpaceInBytes;

            if (includeDetails)
                UsedSizeInBytes += (long)(allocatedSpaceInBytes * report.Density);
        }

        public void AddIndex(FixedSizeTree fst, bool includeDetails)
        {
            var report = StorageReportGenerator.GetReport(fst, includeDetails);
            AddIndex(report, Constants.Storage.PageSize, includeDetails);
        }

        public void AddIndex(Tree tree, bool includeDetails)
        {
            var report = StorageReportGenerator.GetReport(tree, includeDetails);
            AddIndex(report, Constants.Storage.PageSize, includeDetails);
        }

        private void AddIndex(TreeReport report, int pageSize, bool includeDetails)
        {
            Indexes.Add(report);

            var allocatedSpaceInBytes = report.PageCount * pageSize;
            AllocatedSpaceInBytes += allocatedSpaceInBytes;

            if (includeDetails)
                UsedSizeInBytes += (long)(allocatedSpaceInBytes * report.Density);
        }

        public void AddData(RawDataSection section, bool includeDetails)
        {
            var allocatedSpaceInBytes = section.Size;
            AllocatedSpaceInBytes += allocatedSpaceInBytes;
            DataSizeInBytes += allocatedSpaceInBytes;

            if (includeDetails)
                UsedSizeInBytes += (long)(allocatedSpaceInBytes * section.Density);
        }

        public void AddPreAllocatedBuffers(NewPageAllocator tablePageAllocator, bool includeDetails)
        {
            if (PreAllocatedBuffers != null)
                throw new InvalidOperationException("Pre allocated buffers already defined");

            PreAllocatedBuffers = StorageReportGenerator.GetReport(tablePageAllocator, includeDetails);

            AllocatedSpaceInBytes += PreAllocatedBuffers.AllocatedSpaceInBytes;

            if (includeDetails)
            {
                var allocationTree = PreAllocatedBuffers.AllocationTree;
                UsedSizeInBytes += (long)(allocationTree.AllocatedSpaceInBytes * allocationTree.Density);
            }
        }

        public List<TreeReport> Structure { get; }
        public List<TreeReport> Indexes { get; }
        public PreAllocatedBuffersReport PreAllocatedBuffers { get; set; }
        public string Name { get; set; }
        public long NumberOfEntries { get; set; }
        public long DataSizeInBytes { get; private set; }
        public long AllocatedSpaceInBytes { get; private set; }
        public long UsedSizeInBytes { get; private set; }
    }
}
