// -----------------------------------------------------------------------
//  <copyright file="StorageReport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Voron.Debugging
{
    public class StorageReport
    {
        public DataFileReport DataFile { get; set; }
        public List<JournalReport> Journals { get; set; }
        public List<TreeReport> Trees { get; set; }
    }

    public class DataFileReport
    {
        public long AllocatedSpaceInBytes { get; set; }
        public long SpaceInUseInBytes { get; set; }
        public long FreeSpaceInBytes { get; set; }
    }

    public class JournalReport
    {
        public long Number { get; set; }
        public long AllocatedSpaceInBytes { get; set; }
    }

    public class TreeReport
    {
        public string Name { get; set; }
        public long PageCount { get; set; }
        public long EntriesCount { get; set; }
        public long BranchPages { get; set; }
        public long LeafPages { get; set; }
        public long OverflowPages { get; set; }
        public int Depth { get; set; }
        public double Density { get; set; }
        public MultiValuesReport MultiValues { get; set; }
    }

    public class MultiValuesReport
    {
        public long EntriesCount { get; set; }
        public long PageCount { get; set; }
        public long BranchPages { get; set; }
        public long LeafPages { get; set; }
        public long OverflowPages { get; set; }
    }
}
