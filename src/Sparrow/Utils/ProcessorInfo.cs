using System;

namespace Sparrow.Utils
{
    public class ProcessorInfo
    {
        public readonly static int ProcessorCount = GetProcessorCount();

        public static int GetProcessorCount()
        {
            return Environment.ProcessorCount;
        }
    }
}
