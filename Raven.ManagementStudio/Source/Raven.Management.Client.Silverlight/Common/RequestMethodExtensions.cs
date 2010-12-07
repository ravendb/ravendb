namespace Raven.Management.Client.Silverlight.Common
{
    using System;

    /// <summary>
    /// 
    /// </summary>
    public static class RequestMethodExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        public static string GetName(this RequestMethod method)
        {
            return Enum.GetName(typeof (RequestMethod), method);
        }
    }
}