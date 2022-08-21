//-----------------------------------------------------------------------
// <copyright file="ReflectionUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Client.Util
{
    /// <summary>
    /// Helper class for reflection operations
    /// </summary>
    internal static class ReflectionUtil
    {
        internal static class BindingFlagsConstants
        {
            internal const BindingFlags QueryingFields = BindingFlags.Instance | BindingFlags.Public;
        }

        private static Dictionary<Type, string> _fullNameCache = new Dictionary<Type, string>();

        /// <summary>
        /// Gets the full name without version information.
        /// </summary>
        /// <param name="entityType">Type of the entity.</param>
        /// <returns></returns>
        public static string GetFullNameWithoutVersionInformation(Type entityType)
        {
            string result;
            var localFullName = _fullNameCache;
            if (localFullName.TryGetValue(entityType, out result))
                return result;

            var asmName = new AssemblyName(entityType.Assembly.FullName).Name;
            if (entityType.IsGenericType)
            {
                var genericTypeDefinition = entityType.GetGenericTypeDefinition();
                var sb = new StringBuilder(genericTypeDefinition.FullName);
                sb.Append("[");
                bool first = true;
                foreach (var genericArgument in entityType.GetGenericArguments())
                {
                    if (first == false)
                    {
                        sb.Append(", ");
                    }
                    first = false;
                    sb.Append("[")
                        .Append(GetFullNameWithoutVersionInformation(genericArgument))
                        .Append("]");
                }
                sb.Append("], ")
                    .Append(asmName);
                result = sb.ToString();
            }
            else
            {
                result = entityType.FullName + ", " + asmName;
            }

            _fullNameCache = new Dictionary<Type, string>(localFullName)
            {
                {entityType, result}
            };

            return result;
        }

        public static IEnumerable<MemberInfo> GetPropertiesAndFieldsFor<TType>(BindingFlags bindingFlags)
        {
            return GetPropertiesAndFieldsFor(typeof(TType), bindingFlags);
        }

        public static IEnumerable<MemberInfo> GetPropertiesAndFieldsFor(Type type, BindingFlags bindingFlags)
        {
            foreach (var field in type.GetFields(bindingFlags))
            {
                var compilerGeneratedField = field.CustomAttributes.Any(x => x.AttributeType == typeof(CompilerGeneratedAttribute));
                if (compilerGeneratedField)
                    continue;

                yield return field;
            }

            foreach (var property in type.GetProperties(bindingFlags))
            {
                yield return property;
            }
        }

        internal static MemberInfo GetPropertyOrFieldFor(Type type, BindingFlags bindingFlags, string name)
        {
            var field = type.GetField(name, bindingFlags);
            if (field != null)
            {
                var compilerGeneratedField = field.CustomAttributes.Any(x => x.AttributeType == typeof(CompilerGeneratedAttribute));
                if (compilerGeneratedField)
                    return null;

                return field;
            }

            return type.GetProperty(name, bindingFlags);
        }
    }
}
