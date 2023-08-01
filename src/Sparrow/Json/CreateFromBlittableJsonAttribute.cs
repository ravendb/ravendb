using System;

namespace Sparrow.Json
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class CreateFromBlittableJsonAttribute : Attribute
    {
    }
}
