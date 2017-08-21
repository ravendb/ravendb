using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    public enum OrderingType
    {
        String,
        Long,
        Double,
        AlphaNumeric
    }

    internal static class OrderingUtil
    {
        public static OrderingType GetOrderingOfType(Type fieldType)
        {
            var rangeType = DocumentConventions.GetRangeType(fieldType);

            var ordering = OrderingType.String;

            switch (rangeType)
            {
                case RangeType.Double:
                    ordering = OrderingType.Double;
                    break;
                case RangeType.Long:
                    ordering = OrderingType.Long;
                    break;
            }

            return ordering;
        }
    }
}
