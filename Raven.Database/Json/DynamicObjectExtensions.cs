using System;
using Lucene.Net.Documents;

namespace Raven.Database.Json
{
    public static class DynamicObjectExtensions
    {
        public static string ToIndexableString(this DynamicObject self)
        {
            if(self == null)
                return null;

            var val = self.Value;
            if (val is DateTime)
                return DateTools.DateToString((DateTime)val, DateTools.Resolution.DAY);

            if (val is int)
                return NumberTools.LongToString((int)val);

            return val.ToString();
        }
    }
}