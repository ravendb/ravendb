﻿using System;

namespace Sparrow.Utils
{
    internal sealed class ProcessorInfo
    {
        public static readonly int ProcessorCount = GetProcessorCount();

        public static int GetProcessorCount()
        {
            return Environment.ProcessorCount;
        }
    }
}
