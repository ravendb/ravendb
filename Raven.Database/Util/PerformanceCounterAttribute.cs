using System;
using System.Diagnostics;

namespace Raven.Database.Util
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    internal sealed class PerformanceCounterAttribute : Attribute
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public PerformanceCounterType CounterType { get; set; }
    }
}