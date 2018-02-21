namespace Raven.Client.Documents.Indexes
{
    public enum IndexType
    {
        // the values here are PERSISTENT and should not
        // be changed in such a way that they are backward 
        // incompatible

        None                = 0,
        AutoMap             = 1,
        AutoMapReduce       = 2,
        Map                 = 3,
        MapReduce           = 4,
        Faulty              = 5,
        JavaScriptMap       = 6,
        JavaScriptMapReduce = 7,
    }

    internal static class IndexTypeExtensions
    {
        public static bool IsMap(this IndexType self)
        {
            return self == IndexType.Map || self == IndexType.AutoMap || self == IndexType.JavaScriptMap;
        }

        public static bool IsMapReduce(this IndexType self)
        {
            return self == IndexType.MapReduce || self == IndexType.AutoMapReduce || self == IndexType.JavaScriptMapReduce;
        }

        public static bool IsAuto(this IndexType self)
        {
            return self == IndexType.AutoMap || self == IndexType.AutoMapReduce;
        }

        public static bool IsStatic(this IndexType self)
        {
            return self == IndexType.Map || self == IndexType.MapReduce || self == IndexType.JavaScriptMap || self == IndexType.JavaScriptMapReduce;
        }

        public static bool IsJavaScript(this IndexType self)
        {
            return self == IndexType.JavaScriptMap || self == IndexType.JavaScriptMapReduce;
        }
    }
}
