using System;

namespace Sparrow.Json
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class CreateFromBlittableJsonAttribute : Attribute
    {
    }
}
