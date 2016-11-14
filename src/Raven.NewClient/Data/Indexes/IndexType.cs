namespace Raven.NewClient.Client.Data.Indexes
{
    public enum IndexType
    {
        None,
        AutoMap,
        AutoMapReduce,
        Map,
        MapReduce,
        Faulty
    }

    public static class IndexTypeExtensions
    {
        public static bool IsMap(this IndexType self)
        {
            return self == IndexType.Map || self == IndexType.AutoMap;
        }

        public static bool IsMapReduce(this IndexType self)
        {
            return self == IndexType.MapReduce || self == IndexType.AutoMapReduce;
        }

        public static bool IsAuto(this IndexType self)
        {
            return self == IndexType.AutoMap || self == IndexType.AutoMapReduce;
        }

        public static bool IsStatic(this IndexType self)
        {
            return self == IndexType.Map || self == IndexType.MapReduce;
        }
    }
}