using System;
using Sparrow;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class SizeUnitAttribute : Attribute
    {
        public SizeUnit Unit { get; set; }

        public SizeUnitAttribute(SizeUnit unit)
        {
            Unit = unit;
        }
    }
}