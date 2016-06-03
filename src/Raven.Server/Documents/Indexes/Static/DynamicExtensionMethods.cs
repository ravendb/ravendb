using System;
using Raven.Client.Data;

namespace Raven.Server.Documents.Indexes.Static
{
    public static class DynamicExtensionMethods
    {
        public static BoostedValue Boost(dynamic o, object value)
        {
            return new BoostedValue
            {
                Value = o,
                Boost = Convert.ToSingle(value)
            };
        }
    }
}