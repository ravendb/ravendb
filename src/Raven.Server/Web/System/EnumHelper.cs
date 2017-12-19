using System;
using System.ComponentModel;

namespace Raven.Server.Web.System
{
    public static class EnumHelper
    {
        public static string GetDescription(Enum value)
        {
            var fi = value.GetType().GetField(value.ToString());

            if (fi != null)
            {
                var attributes = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
                return (attributes.Length > 0) ? attributes[0].Description : value.ToString();
            }

            return value.ToString();
        }
    }
}
