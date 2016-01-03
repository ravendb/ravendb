// -----------------------------------------------------------------------
//  <copyright file="MemberInfoExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Reflection;

namespace Raven.Abstractions.Extensions
{
    public static class MemberInfoExtensions
    {
        public static bool CanWrite(this MemberInfo memberInfo)
        {
            if (memberInfo.IsProperty())
            {
                return ((PropertyInfo)memberInfo).CanWrite;
            }

            if (memberInfo.IsField())
            {
                return true;
            }

            throw new NotSupportedException("Cannot calculate CanWrite on " + memberInfo);
        }

        public static bool CanRead(this MemberInfo memberInfo)
        {
            if (memberInfo.IsProperty())
            {
                return ((PropertyInfo)memberInfo).CanRead;
            }

            if (memberInfo.IsField())
            {
                return true;
            }

            throw new NotSupportedException("Cannot calculate CanWrite on " + memberInfo);
        }

        public static ParameterInfo[] GetIndexParameters(this MemberInfo memberInfo)
        {
            if (memberInfo.IsProperty())
            {
                return ((PropertyInfo)memberInfo).GetIndexParameters();
            }

            if (memberInfo.IsField())
            {
                return new ParameterInfo[0];
            }

            throw new NotSupportedException("Cannot calculate CanWrite on " + memberInfo);
        }

        public static Type Type(this MemberInfo memberInfo)
        {
            if (memberInfo.IsProperty())
            {
                return ((PropertyInfo)memberInfo).PropertyType;
            }

            if (memberInfo.IsField())
            {
                return ((FieldInfo)memberInfo).FieldType;
            }

            throw new NotSupportedException("Cannot calculate CanWrite on " + memberInfo);
        }

        public static void SetValue(this MemberInfo memberInfo, object entity, object value)
        {
            if (memberInfo.IsProperty())
            {
                ((PropertyInfo)memberInfo).SetValue(entity, value);
                return;
            }

            if (memberInfo.IsField())
            {
                ((FieldInfo)memberInfo).SetValue(entity, value);
                return;
            }

            throw new NotSupportedException("Cannot calculate CanWrite on " + memberInfo);
        }

        public static object GetValue(this MemberInfo memberInfo, object entity)
        {
            if (memberInfo.IsProperty())
            {
                return ((PropertyInfo)memberInfo).GetValue(entity, new object[0]);
            }

            if (memberInfo.IsField())
            {
                return ((FieldInfo)memberInfo).GetValue(entity);
            }

            throw new NotSupportedException("Cannot calculate CanWrite on " + memberInfo);
        }

        public static bool IsProperty(this MemberInfo memberInfo)
        {
            return memberInfo is PropertyInfo;
        }

        public static bool IsField(this MemberInfo memberInfo)
        {
            return memberInfo is FieldInfo;
        }
    }
}
