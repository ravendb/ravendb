//-----------------------------------------------------------------------
// <copyright file="ReflectionUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Linq;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.NewClient.Abstractions.Extensions;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Helper class for reflection operations
    /// </summary>
    public static class ReflectionUtil
    {
        private static Dictionary<Type, string> fullnameCache = new Dictionary<Type, string>();

        /// <summary>
        /// Gets the full name without version information.
        /// </summary>
        /// <param name="entityType">Type of the entity.</param>
        /// <returns></returns>
        public static string GetFullNameWithoutVersionInformation(Type entityType)
        {
            string result;
            var localFullName = fullnameCache;
            if (localFullName.TryGetValue(entityType, out result))
                return result;

            var asmName = new AssemblyName(entityType.Assembly().FullName).Name;
            if (entityType.IsGenericType())
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

            fullnameCache = new Dictionary<Type, string>(localFullName)
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
    }
}
