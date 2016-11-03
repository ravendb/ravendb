// -----------------------------------------------------------------------
//  <copyright file="StorageReport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Voron.Data;
using Voron.Data.Tables;

namespace Voron.Debugging
{
    public class StorageReport
    {
        public DataFileReport DataFile { get; set; }
        public List<JournalReport> Journals { get; set; }
        public List<TreeReport> Trees { get; set; }
        public List<TableReport> Tables { get; set; }
    }

    public class DataFileReport
    {
        public long AllocatedSpaceInBytes { get; set; }
        public long UsedSpaceInBytes { get; set; }
        public long FreeSpaceInBytes { get; set; }
    }

    public class JournalReport
    {
        public long Number { get; set; }
        public long AllocatedSpaceInBytes { get; set; }
    }

    public class TreeReport
    {
        public RootObjectType Type { get; set; }
        public string Name { get; set; }
        public long PageCount { get; set; }
        public long NumberOfEntries { get; set; }
        public long BranchPages { get; set; }
        public long LeafPages { get; set; }
        public long OverflowPages { get; set; }
        public int Depth { get; set; }
        public double Density { get; set; }
        public MultiValuesReport MultiValues { get; set; }

        public long AllocatedSpaceInBytes { get; set; }

        public long UsedSpaceInBytes { get; set; }
    }

    public class MultiValuesReport
    {
        public long NumberOfEntries { get; set; }
        public long PageCount { get; set; }
        public long BranchPages { get; set; }
        public long LeafPages { get; set; }
        public long OverflowPages { get; set; }
    }
}