using System;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class TimeUnitAttribute : Attribute
    {
        public TimeUnit Unit { get; set; }

        public TimeUnitAttribute(TimeUnit unit)
        {
            Unit = unit;
        }
    }
}