using System;
using Raven.Imports.SignalR.Infrastructure;

namespace Raven.Imports.SignalR.Hubs
{
    internal static class HubTypeExtensions
    {
        internal static string GetHubName(this Type type)
        {
            if (!typeof(IHub).IsAssignableFrom(type))
            {
                return null;
            }

            return ReflectionHelper.GetAttributeValue<HubNameAttribute, string>(type, attr => attr.HubName)
                   ?? type.Name;
        } 
    }
}