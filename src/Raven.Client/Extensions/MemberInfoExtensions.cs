// -----------------------------------------------------------------------
//  <copyright file="MemberInfoExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Raven.Client.Extensions
{
    internal static class MemberInfoExtensions
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

        public static void SetValue<T>(this MemberInfo memberInfo, ref T entity, object value)
        {
            if (memberInfo.IsProperty())
            {
                var propertyInfo = (PropertyInfo)memberInfo;
                if (typeof(T).IsValueType)
                {
                    throw new NotSupportedException("Properties are not allowed when using Struct. Use Fields instead.");
                }
                propertyInfo.SetValue(entity, value);
                return;
            }

            if (memberInfo.IsField())
            {
                var fieldInfo = (FieldInfo)memberInfo;
                if (typeof(T).IsValueType)
                {
                    SetStructValue(fieldInfo.Name, fieldInfo.FieldType, ref entity, value);
                    return;
                }
                fieldInfo.SetValue(entity, value);
                return;
            }

            throw new NotSupportedException("Cannot calculate CanWrite on " + memberInfo);
        }

        private static unsafe void SetStructValue<T>(string name, Type type, ref T entity, object value)
        {
            if (type != value.GetType())
                throw new InvalidOperationException($"Can't set type {value.GetType()} to the member '{name}' of type {type}");

            if (value.GetType().IsPrimitive == false)
                throw new InvalidOperationException(
                    $"For struct types, all the members must be primitives, but we try to set the member '{name}' of type {type} with the type {value.GetType()}");

            var offset = Marshal.OffsetOf<T>(name);
            int size = Marshal.SizeOf(entity);
            var arrPtr = stackalloc byte[size];
            Marshal.StructureToPtr(entity, (IntPtr)arrPtr, true);
            var memberAddress = arrPtr + offset.ToInt64();

            switch (value)
            {
                case bool val:
                    *(bool*)memberAddress = val;
                    break;
                case char val:
                    *(char*)memberAddress = val;
                    break;
                case byte val:
                    *memberAddress = val;
                    break;
                case sbyte val:
                    *(sbyte*)memberAddress = val;
                    break;
                case short val:
                    *(short*)memberAddress = val;
                    break;
                case ushort val:
                    *(ushort*)memberAddress = val;
                    break;
                case int val:
                    *(int*)memberAddress = val;
                    break;
                case uint val:
                    *(uint*)memberAddress = val;
                    break;
                case long val:
                    *(long*)memberAddress = val;
                    break;
                case ulong val:
                    *(ulong*)memberAddress = val;
                    break;
                case float val:
                    *(float*)memberAddress = val;
                    break;
                case double val:
                    *(double*)memberAddress = val;
                    break;
                default:
                    throw new ArgumentException($"Unsupported primitive type {value.GetType()}");
            }
            entity = Marshal.PtrToStructure<T>((IntPtr)arrPtr);
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
