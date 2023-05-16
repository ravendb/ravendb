using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;

namespace Raven.Server.Utils
{
    public static class SecurityClearanceValidator
    {
        internal static void AssertSecurityClearance(object input, RavenServer.AuthenticationStatus status)
        {
            if (input == null)
                return;
            if (status == RavenServer.AuthenticationStatus.ClusterAdmin)
                return;

            var members = ReflectionUtil.GetPropertiesAndFieldsFor(input.GetType(), BindingFlags.Public | BindingFlags.Instance);

            foreach (var member in members)
            {
                var type = member.GetMemberType();
                if (type.IsClass && type.IsPrimitive == false && type != typeof(string))
                    AssertSecurityClearance(member.GetValue(input), status);

                var securityClearanceAttribute = member.GetCustomAttribute<SecurityClearanceAttribute>();
                if (securityClearanceAttribute != null)
                    AssertSecurityClearanceLevel(securityClearanceAttribute.SecurityClearanceLevel.ToString(), status.ToString());
            }

        }

        internal static void AssertSecurityClearanceOnGet(object input, RavenServer.AuthenticationStatus status)
        {
            if (input == null)
                return;
            // if (status == RavenServer.AuthenticationStatus.ClusterAdmin)
            //     return;

            var members = ReflectionUtil.GetPropertiesAndFieldsFor(input.GetType(), BindingFlags.Public | BindingFlags.Instance);

            foreach (var member in members)
            {
                var type = member.GetMemberType();
                if (type.IsClass && type.IsPrimitive == false && type != typeof(string))
                    AssertSecurityClearanceOnGet(member.GetValue(input), status);

                var securityClearanceAttribute = member.GetCustomAttribute<SecurityClearanceAttribute>();
                if (securityClearanceAttribute != null)
                    Console.WriteLine("fdfdf");
            }
        }


        private static void AssertSecurityClearanceLevel(string attributeStatus, string userStatus)
        {
            if (attributeStatus == userStatus)
                throw new AuthenticationException(
                    $"Bad security clearance: '{userStatus}'. The current user does not have the necessary security clearance. " +
                    $"External script execution is only allowed for users with '{RavenServer.AuthenticationStatus.Operator}' or higher security clearance.");
        }
    }

}
