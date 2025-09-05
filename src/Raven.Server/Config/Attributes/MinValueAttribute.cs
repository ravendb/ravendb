using System;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public sealed class MinValueAttribute : Attribute
    {
        public int Int32Value { get; set; }

        public MinValueAttribute(int value)
        {
            Int32Value = value;
        }
    }
}