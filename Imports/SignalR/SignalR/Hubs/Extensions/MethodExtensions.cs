using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Imports.SignalR.Hubs
{
    public static class MethodExtensions
    {
        public static bool Matches(this MethodDescriptor methodDescriptor, params IJsonValue[] parameters)
        {
            if ((methodDescriptor.Parameters.Count > 0 && parameters == null)
                || methodDescriptor.Parameters.Count != parameters.Length)
            {
                return false;
            }

            return true;
        }
    }
}
