//-----------------------------------------------------------------------
// <copyright file="ReflectionExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

using System.Reflection;

namespace Raven.Abstractions.Extensions
{
    public static class ReflectionExtensions
    {
        public static void InvokeMember(this Type type, string name, BindingFlags invokeAttr, object target)
        {
            var method = type.GetMethod(name);
            method.Invoke(target, null);
        }

        public static Type GetMemberType(this MemberInfo member)
        {
            var propertyInfo = member as PropertyInfo;
            if (propertyInfo != null)
                return propertyInfo.PropertyType;

            var fieldInfo = member as FieldInfo;
            if (fieldInfo != null)
                return fieldInfo.FieldType;

            throw new NotSupportedException(member.GetType().ToString());
        }

        private static bool TestAccessibility(MethodBase member, BindingFlags bindingFlags)
        {
            bool visibility = (member.IsPublic && bindingFlags.HasFlag(BindingFlags.Public)) ||
              (!member.IsPublic && bindingFlags.HasFlag(BindingFlags.NonPublic));

            bool instance = (member.IsStatic && bindingFlags.HasFlag(BindingFlags.Static)) ||
              (!member.IsStatic && bindingFlags.HasFlag(BindingFlags.Instance));

            return visibility && instance;
        }
    }
}
