using System;


namespace Raven.NewClient.Client.Util
{
    public static class Types
    {
        public static bool IsEntityType(this Type type)
        {
            //TODO  - replace RavenJObject
            //return type != typeof (object) && type != typeof (RavenJObject);
            return type != typeof (object);
        }
    }
}
