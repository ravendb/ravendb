using System;

namespace Sparrow.Json
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    internal sealed class CreateFromBlittableJsonAttribute : Attribute
    {
    }
}
