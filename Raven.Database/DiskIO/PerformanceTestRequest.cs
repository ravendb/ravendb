// -----------------------------------------------------------------------
//  <copyright file="PerformanceTestRequest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.DiskIO
{
    public abstract class AbstractPerformanceTestRequest
    {
        public abstract string TestType { get; }
        public string Path { get; set; }
        public long FileSize { get; set; }
        
    }

    public class GenericPerformanceTestRequest : AbstractPerformanceTestRequest
    {
        public const string Mode = "generic";

        public override string TestType { get { return Mode; } }

        public OperationType OperationType { get; set; }
        public BufferingType BufferingType { get; set; }
        public int TimeToRunInSeconds { get; set; }
        public bool Sequential { get; set; }
        public int ThreadCount { get; set; }
        public int? RandomSeed { get; set; }
        private int chunkSize;

        public int ChunkSize
        {
            get { return chunkSize; }
            set
            {
                if (value % 4096 != 0)
                {
                    throw new ArgumentException("ChunkSize must be multiply of 4KB");
                }
                chunkSize = value;
            }
        }

        public GenericPerformanceTestRequest()
        {
            chunkSize = 4 * 1024;
            Sequential = false;
            FileSize = 1024 * 1024 * 1024;
            BufferingType = BufferingType.None;
            TimeToRunInSeconds = 30;
            ThreadCount = Environment.ProcessorCount;
        }

        public bool BufferedReads
        {
            get { return BufferingType == BufferingType.Read || BufferingType == BufferingType.ReadAndWrite; }
        }

        public bool BufferedWrites
        {
            get { return BufferingType == BufferingType.ReadAndWrite; }
        }

    }

    public class BatchPerformanceTestRequest : AbstractPerformanceTestRequest
    {
        public const string Mode = "batch";

        public override string TestType { get { return Mode; } }
        public int NumberOfDocuments { get; set; }
        public int SizeOfDocuments { get; set; }
        public int NumberOfDocumentsInBatch { get; set; }
        public int WaitBetweenBatches { get; set; }
    }

    public enum OperationType
    {
        Read,
        Write,
        Mix
    }

    public enum BufferingType
    {
        None,
        ReadAndWrite, 
        Read
    }
}