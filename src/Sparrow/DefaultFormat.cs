namespace Sparrow
{
    internal static class DefaultFormat
    {
        public static readonly string TimeOnlyFormatToWrite = "o";
        public static readonly string DateOnlyFormatToWrite = "o";
        public static readonly string DateTimeOffsetFormatsToWrite = "o";
        public static readonly string DateTimeFormatsToWrite = "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff";

        public static readonly string[] OnlyDateTimeFormat = {
            "yyyy'-'MM'-'dd'T'HH':'mm':'ss",
            DateTimeFormatsToWrite,
            "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'Z'"
        };

        /// <remarks>
        /// 'r' format is used on the in metadata, because it's delivered as http header. 
        /// </remarks>
        public static readonly string[] DateTimeFormatsToRead = {
            DateTimeOffsetFormatsToWrite,
            DateTimeFormatsToWrite,
            "yyyy-MM-ddTHH:mm:ss.fffffffzzz",
            "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
            "r",
            "yyyy-MM-ddTHH:mm:ss.fffK",
            "yyyy-MM-ddTHH:mm:ss.FFFK",
        };
    }
}
