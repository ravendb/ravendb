namespace Raven.Client.Silverlight.Common
{
    using System;

    public static class RequestMethodExtensions
    {
        public static string GetName(this RequestMethod method)
        {
            return Enum.GetName(typeof(RequestMethod), method);
        }
    }
}
