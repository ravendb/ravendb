//-----------------------------------------------------------------------
// <copyright file="TypeSystem.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Raven.Client.Linq
{
    internal static class TypeSystem
    {
        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
                return null;
            if (seqType.IsArray)
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            if (seqType.GetTypeInfo().IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                        return ienum;
                }
            }
            var ifaces = seqType.GetInterfaces();
            if (ifaces != null && ifaces.Any())
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null)
                        return ienum;
                }
            }
            if (seqType.GetTypeInfo().BaseType != null && seqType.GetTypeInfo().BaseType != typeof(object))
                return FindIEnumerable(seqType.GetTypeInfo().BaseType);
            return null;
        }

        internal static Type GetElementType(Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);
            if (ienum == null)
                return seqType;
            return ienum.GetGenericArguments()[0];
        }

        internal static bool IsNullableType(Type type)
        {
            return type != null && type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        internal static Type GetNonNullableType(Type type)
        {
            if (IsNullableType(type))
                return type.GetGenericArguments()[0];
            return type;
        }
    }
}
