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

        public void AddStructure(FixedSizeTree fst, bool calculateExactSizes)
        {
            var report = StorageReportGenerator.GetReport(fst, calculateExactSizes);
            AddStructure(report, Constants.Storage.PageSize, calculateExactSizes);
        }

        public void AddStructure(Tree tree, bool calculateExactSizes)
        {
            var report = StorageReportGenerator.GetReport(tree, calculateExactSizes);
            AddStructure(report, Constants.Storage.PageSize, calculateExactSizes);
        }

        private void AddStructure(TreeReport report, int pageSize, bool calculateExactSizes)
        {
            Structure.Add(report);

            var allocatedSpaceInBytes = report.PageCount * pageSize;
            AllocatedSpaceInBytes += allocatedSpaceInBytes;

            if (calculateExactSizes)
                UsedSizeInBytes += (long)(allocatedSpaceInBytes * report.Density);
        }

        public void AddIndex(FixedSizeTree fst, bool calculateExactSizes)
        {
            var report = StorageReportGenerator.GetReport(fst, calculateExactSizes);
            AddIndex(report, Constants.Storage.PageSize, calculateExactSizes);
        }

        public void AddIndex(Tree tree, bool calculateExactSizes)
        {
            var report = StorageReportGenerator.GetReport(tree, calculateExactSizes);
            AddIndex(report, Constants.Storage.PageSize, calculateExactSizes);
        }

        private void AddIndex(TreeReport report, int pageSize, bool calculateExactSizes)
        {
            Indexes.Add(report);

            var allocatedSpaceInBytes = report.PageCount * pageSize;
            AllocatedSpaceInBytes += allocatedSpaceInBytes;

            if (calculateExactSizes)
                UsedSizeInBytes += (long)(allocatedSpaceInBytes * report.Density);
        }

        public void AddData(RawDataSection section, bool calculateExactSizes)
        {
            var allocatedSpaceInBytes = section.Size;
            AllocatedSpaceInBytes += allocatedSpaceInBytes;
            DataSizeInBytes += allocatedSpaceInBytes;

            if (calculateExactSizes)
                UsedSizeInBytes += (long)(allocatedSpaceInBytes * section.Density);
        }

        public void AddPreAllocatedBuffers(NewPageAllocator tablePageAllocator, bool calculateExactSizes)
        {
            if (PreAllocatedBuffers != null)
                throw new InvalidOperationException("Pre allocated buffers already defined");

            PreAllocatedBuffers = StorageReportGenerator.GetReport(tablePageAllocator, calculateExactSizes);

            AllocatedSpaceInBytes += PreAllocatedBuffers.AllocatedSpaceInBytes;

            if (calculateExactSizes)
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