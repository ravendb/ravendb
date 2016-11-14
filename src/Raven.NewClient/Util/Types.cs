using System;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Util
{
    public static class Types
    {
        public static bool IsEntityType(this Type type)
        {
            return type != typeof (object) && type != typeof (RavenJObject);
        }
    }
}
