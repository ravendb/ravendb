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
        }
    }
}
