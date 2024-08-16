namespace Raven.Server.EventListener;

public class EventListener
{
    public class Constants
    {
        public class EventNames
        {
            public class GC
            {
                public const string GCStart = "GCStart_V2";
                public const string GCEnd = "GCEnd_V1";
                public const string GCSuspendBegin = "GCSuspendEEBegin_V1";
                public const string GCSuspendEnd = "GCSuspendEEEnd_V1";
                public const string GCRestartBegin = "GCRestartEEBegin_V1";
                public const string GCRestartEnd = "GCRestartEEEnd_V1";
                public const string GCFinalizersBegin = "GCFinalizersBegin_V1";
                public const string GCFinalizersEnd = "GCFinalizersEnd_V1";
            }

            public class Allocations
            {
                public const string Allocation = "GCAllocationTick_V4";
            }

            public class Contention
            {
                public const string ContentionStart = "ContentionStart";
                public const string ContentionStop = "ContentionStop";
            }

            public class Threads
            {
                public const string ThreadPoolWorkerThreadStart = "ThreadPoolWorkerThreadStart";
                public const string ThreadPoolWorkerThreadWait = "ThreadPoolWorkerThreadWait";
                public const string ThreadPoolWorkerThreadStop = "ThreadPoolWorkerThreadStop";
                public const string ThreadPoolMinMaxThreads = "ThreadPoolMinMaxThreads";
                public const string ThreadPoolWorkerThreadAdjustmentAdjustment = "ThreadPoolWorkerThreadAdjustmentAdjustment";
                public const string ThreadPoolWorkerThreadAdjustmentSample = "ThreadPoolWorkerThreadAdjustmentSample";
                public const string ThreadPoolWorkerThreadAdjustmentStats = "ThreadPoolWorkerThreadAdjustmentStats";
                public const string ThreadCreating = "ThreadCreating";
                public const string ThreadCreated = "ThreadCreated";
                public const string ThreadRunning = "ThreadRunning";
                public const string GCCreateConcurrentThread_V1 = "GCCreateConcurrentThread_V1";
            }
        }
    }
}
