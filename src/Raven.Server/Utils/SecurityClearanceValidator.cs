using System.Collections;
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

            var inputType = input.GetType();
            if (inputType.IsClass == false || inputType.IsPrimitive || inputType == typeof(string))
                return;

            var members = ReflectionUtil.GetPropertiesAndFieldsFor(inputType, BindingFlags.Public | BindingFlags.Instance);

            foreach (var member in members)
            {
                var type = member.GetMemberType();
                var value = member.GetValue(input);

                if (type != typeof(string))
                {
                    if (value is IEnumerable enumerable)
                    {
                        foreach (var o in enumerable)
                            AssertSecurityClearance(o, status);
                    }
                    else if (type.IsClass && type.IsPrimitive == false)
                        AssertSecurityClearance(value, status);
                }

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
                case (SecurityClearance.ClusterAdmin, RavenServer.AuthenticationStatus.Operator):
                case (SecurityClearance.ClusterAdmin, RavenServer.AuthenticationStatus.Allowed):
                    throw new AuthenticationException(
                        $"Bad security clearance: '{userStatus}'. The current user does not have the necessary security clearance. " +
                        $"This operation is only allowed for users with '{attributeStatus}' or higher security clearance.");
                default:
                    return;
            }
        }
    }
}
