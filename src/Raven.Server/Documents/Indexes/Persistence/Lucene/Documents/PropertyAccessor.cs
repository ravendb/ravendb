using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Microsoft.CSharp.RuntimeBinder;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public delegate object DynamicGetter(object target);

    public sealed class PropertyAccessor : IPropertyAccessor
    {
        private readonly Dictionary<string, Accessor> Properties = new();

        private readonly List<KeyValuePair<string, Accessor>> _propertiesInOrder = new();


        public struct PropertiesEnumerator : IEnumerator<PropertyItem>
        {
            private readonly List<KeyValuePair<string, Accessor>> _propertiesInOrder;
            private readonly object _target;
            private int _currentIdx;

            public PropertiesEnumerator(List<KeyValuePair<string, Accessor>> properties, object target)
            {
                _propertiesInOrder = properties;
                _target = target;
                _currentIdx = -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                _currentIdx++;
                return _currentIdx < _propertiesInOrder.Count;
            }

            public void Reset()
            {
                _currentIdx = -1;
            }

            object IEnumerator.Current => Current;

            public PropertyItem Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    var (key, value) = _propertiesInOrder[_currentIdx];
                    return new PropertyItem(key, value.GetValue(_target), value.GroupByField, value.IsGroupByField);
                }
            }

            public void Dispose() {}
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<PropertyItem> GetProperties(object target)
        {
            return new PropertiesEnumerator(_propertiesInOrder, target);
        }


        public static IPropertyAccessor Create(Type type, object instance)
        {
            if (type == typeof(JsObject))
                return new JintPropertyAccessor(null);

            if (instance is Dictionary<string, object> dict)
                return DictionaryAccessor.Create(dict);

            return new PropertyAccessor(type);
        }

        public object GetValue(string name, object target)
        {
            if (Properties.TryGetValue(name, out Accessor accessor))
                return accessor.GetValue(target);

            throw new InvalidOperationException($"The {name} property was not found");
        }

        private PropertyAccessor(Type type, Dictionary<string, CompiledIndexField> groupByFields = null)
        {
            var isValueType = type.IsValueType;
            foreach (var prop in type.GetProperties())
            {
                var getMethod = isValueType
                    ? (Accessor)CreateGetMethodForValueType(prop, type)
                    : CreateGetMethodForClass(prop, type);

                if (groupByFields != null)
                {
                    foreach (var groupByField in groupByFields.Values)
                    {
                        if (groupByField.IsMatch(prop.Name) == false) 
                            continue;

                        getMethod.GroupByField = groupByField;
                        getMethod.IsGroupByField = true;
                        break;
                    }
                }

                Properties.Add(prop.Name, getMethod);
                _propertiesInOrder.Add(new KeyValuePair<string, Accessor>(prop.Name, getMethod));
            }
        }

        private static ValueTypeAccessor CreateGetMethodForValueType(PropertyInfo prop, Type type)
        {
            var binder = Microsoft.CSharp.RuntimeBinder.Binder.GetMember(CSharpBinderFlags.None, prop.Name, type, new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) });
            return new ValueTypeAccessor(CallSite<Func<CallSite, object, object>>.Create(binder));
        }

        private static ClassAccessor CreateGetMethodForClass(PropertyInfo propertyInfo, Type type)
        {
            var getMethod = propertyInfo.GetGetMethod();

            if (getMethod == null)
                throw new InvalidOperationException($"Could not retrieve GetMethod for the {propertyInfo.Name} property of {type.FullName} type");

            var arguments = new[]
            {
                typeof (object)
            };

            var getterMethod = new DynamicMethod(string.Concat("_Get", propertyInfo.Name, "_"), typeof(object), arguments, propertyInfo.DeclaringType);
            var generator = getterMethod.GetILGenerator();

            generator.DeclareLocal(typeof(object));
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Castclass, propertyInfo.DeclaringType);
            generator.EmitCall(OpCodes.Callvirt, getMethod, null);

            if (propertyInfo.PropertyType.IsClass == false)
                generator.Emit(OpCodes.Box, propertyInfo.PropertyType);

            generator.Emit(OpCodes.Ret);

            return new ClassAccessor((DynamicGetter)getterMethod.CreateDelegate(typeof(DynamicGetter)));
        }

        private sealed class ValueTypeAccessor : Accessor
        {
            private readonly CallSite<Func<CallSite, object, object>> _callSite;

            public ValueTypeAccessor(CallSite<Func<CallSite, object, object>> callSite)
            {
                _callSite = callSite;
            }

            public override object GetValue(object target)
            {
                return _callSite.Target(_callSite, target);
            }
        }

        private sealed class ClassAccessor : Accessor
        {
            private readonly DynamicGetter _dynamicGetter;

            public ClassAccessor(DynamicGetter dynamicGetter)
            {
                _dynamicGetter = dynamicGetter;
            }

            public override object GetValue(object target)
            {
                return _dynamicGetter(target);
            }
        }

        public abstract class Accessor
        {
            public abstract object GetValue(object target);

            public bool IsGroupByField;

            public CompiledIndexField GroupByField;
        }

        internal static IPropertyAccessor CreateMapReduceOutputAccessor(Type type, object instance, Dictionary<string, CompiledIndexField> groupByFields, bool isObjectInstance = false)
        {
            if (isObjectInstance || type == typeof(JsObject) || type.IsSubclassOf(typeof(ObjectInstance)))
                return new JintPropertyAccessor(groupByFields);

            if (instance is Dictionary<string, object> dict)
                return DictionaryAccessor.Create(dict, groupByFields);

            return new PropertyAccessor(type, groupByFields);
        }
    }

    internal sealed class JintPropertyAccessor : IPropertyAccessor
    {
        private readonly Dictionary<string, CompiledIndexField> _groupByFields;

        public JintPropertyAccessor(Dictionary<string, CompiledIndexField> groupByFields)
        {
            _groupByFields = groupByFields;
        }

        internal struct JintPropertiesEnumerator : IEnumerator<PropertyItem>
        {
            private readonly ObjectInstance _objectInstance;
            private readonly Dictionary<string, CompiledIndexField> _groupByFields;
            private IEnumerator<KeyValuePair<JsValue, PropertyDescriptor>> _enumerable;

            internal JintPropertiesEnumerator(ObjectInstance oi, Dictionary<string, CompiledIndexField> groupByFields)
            {
                _objectInstance = oi;
                _groupByFields = groupByFields;
                _enumerable = oi.GetOwnProperties().GetEnumerator();
            }

            public bool MoveNext()
            {
                return _enumerable.MoveNext();
            }

            public void Reset()
            {
                _enumerable = _objectInstance.GetOwnProperties().GetEnumerator();
            }

            object IEnumerator.Current => Current;

            public PropertyItem Current
            {
                get
                {
                    var property = _enumerable.Current;
                    var propertyAsString = property.Key.AsString();

                    CompiledIndexField @field = null;
                    var isGroupByField = _groupByFields?.TryGetValue(propertyAsString, out @field) ?? false;

                    return new PropertyItem(propertyAsString, GetValue(property.Value.Value), @field, isGroupByField);
                }
            }

            public void Dispose() { }
        }


        public IEnumerator<PropertyItem> GetProperties(object target)
        {
            if (!(target is ObjectInstance oi))
                throw new ArgumentException($"JintPropertyAccessor.GetPropertiesInOrder is expecting a target of type ObjectInstance but got one of type {target.GetType().Name}.");

            return new JintPropertiesEnumerator(oi, _groupByFields);
        }

        public object GetValue(string name, object target)
        {
            if (!(target is ObjectInstance oi))
                throw new ArgumentException($"JintPropertyAccessor.GetValue is expecting a target of type ObjectInstance but got one of type {target.GetType().Name}.");
            if (oi.HasOwnProperty(name) == false)
                throw new MissingFieldException($"The target for 'JintPropertyAccessor.GetValue' doesn't contain the property {name}.");
            return GetValue(oi.GetOwnProperty(name).Value);
        }

        private static object GetValue(JsValue jsValue)
        {
            if (jsValue.IsNull())
                return null;
            if (jsValue.IsString())
                return jsValue.AsString();
            if (jsValue.IsBoolean())
                return jsValue.AsBoolean();
            if (jsValue.IsNumber())
                return jsValue.AsNumber();
            if (jsValue.IsDate())
                return jsValue.AsDate();
            if (jsValue is ObjectWrapper ow)
            {
                var target = ow.Target;
                switch (target)
                {
                    case LazyStringValue lsv:
                        return lsv;

                    case LazyCompressedStringValue lcsv:
                        return lcsv;

                    case LazyNumberValue lnv:
                        return lnv; //should be already blittable supported type.
                }
                ThrowInvalidObject(jsValue);
            }
            else if (jsValue.IsArray())
            {
                var arr = jsValue.AsArray();
                var array = new object[arr.Length];
                var i = 0;
                foreach (var val in arr)
                {
                    array[i++] = GetValue(val);
                }

                return array;
            }
            else if (jsValue.IsObject())
            {
                return jsValue.AsObject();
            }
            if (jsValue.IsUndefined())
            {
                return null;
            }

            ThrowInvalidObject(jsValue);
            return null;
        }

        [DoesNotReturn]
        private static void ThrowInvalidObject(JsValue jsValue)
        {
            throw new NotSupportedException($"Was requested to extract the value out of a JsValue object but could not figure its type, value={jsValue}");
        }
    }
}
