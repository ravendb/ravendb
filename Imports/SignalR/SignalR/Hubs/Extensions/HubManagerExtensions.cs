using System;
using System.Globalization;

namespace Raven.Imports.SignalR.Hubs
{
    public static class HubManagerExtensions
    {
        public static HubDescriptor EnsureHub(this IHubManager hubManager, string hubName)
        {
            var descriptor = hubManager.GetHub(hubName);

            if (descriptor == null)
            {
                throw new InvalidOperationException(String.Format(CultureInfo.CurrentCulture, "'{0}' hub could not be resolved.", hubName));
            }

            return descriptor;
        }
    }
}
