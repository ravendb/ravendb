﻿namespace Raven.Server.ServerWide.Memory
{
    public sealed class ProcessMemoryUsage
    {
        public ProcessMemoryUsage(long workingSet, long privateMemory)
        {
            WorkingSet = workingSet;
            PrivateMemory = privateMemory;
        }

        public readonly long WorkingSet;

        public readonly long PrivateMemory;
    }
}