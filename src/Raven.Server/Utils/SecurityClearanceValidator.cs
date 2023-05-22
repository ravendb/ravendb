using System.Reflection;
using System.Security.Authentication;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;

namespace Raven.Server.Utils
{
    public static class SecurityClearanceValidator
    {
        internal static void AssertSecurityClearance(object input, RavenServer.AuthenticationStatus? status)
        {
            if (input == null || status == null || status == RavenServer.AuthenticationStatus.ClusterAdmin)
                return;

            var members = ReflectionUtil.GetPropertiesAndFieldsFor(input.GetType(), BindingFlags.Public | BindingFlags.Instance);

            foreach (var member in members)
            {
                var type = member.GetMemberType();
                if (type.IsClass && type.IsPrimitive == false && type != typeof(string))
                    AssertSecurityClearance(member.GetValue(input), status);

                var securityClearanceAttribute = member.GetCustomAttribute<SecurityClearanceAttribute>();
                if (securityClearanceAttribute != null)
                    AssertSecurityClearanceLevel(securityClearanceAttribute.SecurityClearanceLevel, (RavenServer.AuthenticationStatus)status);
            }
        }

        private static void AssertSecurityClearanceLevel(SecurityClearance attributeStatus, RavenServer.AuthenticationStatus userStatus)
        {
            switch (attributeStatus, userStatus)
            {
                case (SecurityClearance.Operator, RavenServer.AuthenticationStatus.Allowed):
                    throw new AuthenticationException(
                        $"Bad security clearance: '{userStatus}'. The current user does not have the necessary security clearance. " +
                        $"This operation is only allowed for users with '{attributeStatus}' or higher security clearance.");
                default:
                    return;
            }
        }
    }
}
