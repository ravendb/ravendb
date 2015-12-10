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
        private const BindingFlags DefaultFlags = BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance;

        public static bool IsGenericTypeDefinition(this Type type)
        {
#if !DNXCORE50
            return type.IsGenericTypeDefinition;
#else
            return type.GetTypeInfo().IsGenericTypeDefinition;
#endif
        }

        public static bool IsDefined(this Type type, Type attributeType, bool inherit)
        {
#if !DNXCORE50
            return type.IsDefined(attributeType, inherit);
#else
            return type.GetTypeInfo().CustomAttributes.Any(a => a.AttributeType == attributeType);
#endif
        }

        public static bool IsEnum(this Type type)
        {
#if !DNXCORE50
            return type.IsEnum;
#else
            return type.GetTypeInfo().IsEnum;
#endif
        }

        public static bool IsValueType(this Type type)
        {
#if !DNXCORE50
            return type.IsValueType;
#else
            return type.GetTypeInfo().IsValueType;
#endif
        }

        public static Type BaseType(this Type type)
        {
#if !DNXCORE50
            return type.BaseType;
#else
            return type.GetTypeInfo().BaseType;
#endif
        }

        public static MethodInfo GetMethod(this Type type, string name, IList<Type> parameterTypes)
        {
            return type.GetMethod(name, DefaultFlags, null, parameterTypes, null);
        }

        public static MethodInfo GetMethod(this Type type, string name, BindingFlags bindingFlags, object placeHolder1, IList<Type> parameterTypes, object placeHolder2)
        {
            return type
                .GetTypeInfo()
                .DeclaredMethods
                .Where(m =>
                {
                    if (name != null && m.Name != name)
                        return false;

                    if (!TestAccessibility(m, bindingFlags))
                        return false;

                    return m
                    .GetParameters()
                    .Select(p => p.ParameterType)
                    .SequenceEqual(parameterTypes);
                })
                .SingleOrDefault();
        }

        public static void InvokeMember(this Type type, string name, BindingFlags invokeAttr, object target)
        {
#if !DNXCORE50
            type.InvokeMember(name, invokeAttr, null, target, null);
#else
            var method = type.GetMethod(name);
            method.Invoke(target, null);
#endif
        }

        public static Assembly Assembly(this Type type)
        {
#if !DNXCORE50
            return type.Assembly;
#else
            return type.GetTypeInfo().Assembly;
#endif
        }

        public static bool IsGenericType(this Type type)
        {
#if !(NETFX_CORE || PORTABLE)
            return type.IsGenericType;
#else
            return type.GetTypeInfo().IsGenericType;
#endif
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
