//-----------------------------------------------------------------------
// <copyright file="ReflectionExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Reflection;

namespace Raven.Client.Extensions
{
    public static class ReflectionExtensions
    {
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
    }
}
