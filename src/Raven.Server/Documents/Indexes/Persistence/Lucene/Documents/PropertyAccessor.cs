using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public delegate object DynamicGetter(object target);

    public class PropertyAccessor : IPropertyAccessor
    {
        protected readonly Dictionary<string, Accessor> Properties = new Dictionary<string, Accessor>();

        protected readonly List<KeyValuePair<string, Accessor>> _propertiesInOrder =
            new List<KeyValuePair<string, Accessor>>();

        public static IPropertyAccessor Create(Type type, object instance, JavaScriptEngineType? engineType)
        {
            if (instance is JsHandleJint jint)
            {
                var tt = jint.Item.GetType();
                if (tt == typeof(ObjectInstance) || tt.IsSubclassOf(typeof(ObjectInstance)))
                {
                    return new JsPropertyAccessorJint(null);
                }
            }
            else if (instance is JsHandleV8)
            {
                //TODO: egor cehck this
                if (typeof(IObjectInstance<>).IsAssignableFrom(type))
                {
                    return new JsPropertyAccessorV8(null);
                }
            }

            if (instance is Dictionary<string, object> dict)
                return DictionaryAccessor.Create(dict);

            return new PropertyAccessor(type);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IPropertyAccessor CreateMapReduceOutputAccessor(Type type, object instance, Dictionary<string, CompiledIndexField> groupByFields, JavaScriptEngineType engineType, bool isObjectInstance = false)
        {
            if(instance is JsHandleJint jint)
            {
                var tt = jint.Item.GetType();
                if(isObjectInstance || tt == typeof(ObjectInstance) || tt.IsSubclassOf(typeof(ObjectInstance)))
                {
                    return new JsPropertyAccessorJint(groupByFields);
                }
            } else if (instance is JsHandleV8)
            {
                if (isObjectInstance || typeof(IObjectInstance<>).IsAssignableFrom(type))
                {
                    return new JsPropertyAccessorV8(groupByFields);
                }
            }
            ////TODO: egor check this
            //if (isObjectInstance || typeof(IObjectInstance<>).IsAssignableFrom(type))
            //{
            //    if (typeof(IObjectInstance<>).IsAssignableFrom(type))
            //    {
            //        switch (engineType)
            //        {
            //            case JavaScriptEngineType.Jint:
            //                return new JsPropertyAccessorJint(null);
            //              //  return new JsPropertyAccessorJint(groupByFields);
            //            case JavaScriptEngineType.V8:
            //                return new JsPropertyAccessorV8(null);
            //            default:
            //                throw new ArgumentOutOfRangeException(nameof(engineType), engineType, null);
            //        }
            //    }
            //}
            //else if (isObjectInstance || type == typeof(ObjectInstance) || type.IsSubclassOf(typeof(ObjectInstance)))
            //{

            //}

                if (instance is Dictionary<string, object> dict)
                return DictionaryAccessor.Create(dict, groupByFields);

            if (type == null)
                type = instance.GetType();
            return new PropertyAccessor(type, groupByFields);
        }

        protected PropertyAccessor(Type type, Dictionary<string, CompiledIndexField> groupByFields = null)
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
                        if (groupByField.IsMatch(prop.Name))
                        {
                            getMethod.GroupByField = groupByField;
                            getMethod.IsGroupByField = true;
                            break;
                        }
                    }
                }

                Properties.Add(prop.Name, getMethod);
                _propertiesInOrder.Add(new KeyValuePair<string, Accessor>(prop.Name, getMethod));
            }
        }

        public IEnumerable<(string Key, object Value, CompiledIndexField GroupByField, bool IsGroupByField)> GetPropertiesInOrder(object target)
        {
            foreach ((var key, var value) in _propertiesInOrder)
            {
                yield return (key, value.GetValue(target), value.GroupByField, value.IsGroupByField);
            }
        }

        public object GetValue(string name, object target)
        {
            if (Properties.TryGetValue(name, out Accessor accessor))
                return accessor.GetValue(target);

            throw new InvalidOperationException(string.Format("The {0} property was not found", name));
        }

        protected static ValueTypeAccessor CreateGetMethodForValueType(PropertyInfo prop, Type type)
        {
            var binder = Microsoft.CSharp.RuntimeBinder.Binder.GetMember(CSharpBinderFlags.None, prop.Name, type, new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) });
            return new ValueTypeAccessor(CallSite<Func<CallSite, object, object>>.Create(binder));
        }

        protected static ClassAccessor CreateGetMethodForClass(PropertyInfo propertyInfo, Type type)
        {
            var getMethod = propertyInfo.GetGetMethod();

            if (getMethod == null)
                throw new InvalidOperationException(string.Format("Could not retrieve GetMethod for the {0} property of {1} type", propertyInfo.Name, type.FullName));

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

        protected class ValueTypeAccessor : Accessor
        {
            protected readonly CallSite<Func<CallSite, object, object>> _callSite;

            public ValueTypeAccessor(CallSite<Func<CallSite, object, object>> callSite)
            {
                _callSite = callSite;
            }

            public override object GetValue(object target)
            {
                return _callSite.Target(_callSite, target);
            }
        }

        protected class ClassAccessor : Accessor
        {
            protected readonly DynamicGetter _dynamicGetter;

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
    }

    public abstract class JsPropertyAccessor<T> : IPropertyAccessor
    where T : struct, IJsHandle<T>
    {
        protected readonly Dictionary<string, CompiledIndexField> _groupByFields;

        protected JsPropertyAccessor(Dictionary<string, CompiledIndexField> groupByFields)
        {
            _groupByFields = groupByFields;
        }

        protected abstract void AssertTargetType(object target, [CallerMemberName] string caller = null);

        public IEnumerable<(string Key, object Value, CompiledIndexField GroupByField, bool IsGroupByField)> GetPropertiesInOrder(object target)
        {
            AssertTargetType(target);

            //the case should be safe because we asserted the type before
            T jsHandle = (T)target;
            if (jsHandle.IsObject == false)
                ThrowArgumentException(target);

            foreach (var property in jsHandle.GetOwnProperties())
            {
                CompiledIndexField field = null;
                var isGroupByField = _groupByFields?.TryGetValue(property.Key, out field) ?? false;

                yield return (property.Key, GetValue(property.Value), field, isGroupByField);
            }
        }

        private static void ThrowArgumentException(object target)
        {
            throw new ArgumentException(
                $"JsPropertyAccessor.GetPropertiesInOrder is expecting a target of type of IJsHandle and IsObject == true but got one of type {target.GetType().Name} and IsObject == false");
        }

        public object GetValue(string name, object target)
        {
            AssertTargetType(target);
            T jsHandle = (T)target;
            if (jsHandle.IsObject == false)
                ThrowArgumentException(target);
            if (jsHandle.HasOwnProperty(name) == false)
                throw new MissingFieldException($"The target for 'JintPropertyAccessor.GetValue' doesn't contain the property {name}.");

            return GetValue(jsHandle.GetProperty(name));
        }

        private object GetValue(T jsValue)
        {
            if (jsValue.IsNull)
                return null;
            if (jsValue.IsUndefined)
                return null;
            if (jsValue.IsStringEx)
                return jsValue.AsString;
            if (jsValue.IsBoolean)
                return jsValue.AsBoolean;
            if (jsValue.IsInt32)
                return jsValue.AsInt32;
            if (jsValue.IsNumberEx)
                return jsValue.AsDouble;
            if (jsValue.IsDate)
                return jsValue.AsDate; //TODO: egor do we need this: /*.ToDateTime()*/ ?

            if (jsValue.IsArray)
            {
                var array = new object[jsValue.ArrayLength];
                for (int i = 0; i < jsValue.ArrayLength; i++)
                {
                    using (var prop = jsValue.GetProperty(i))
                    {
                        array[i++] = GetValue(prop);
                    }
                }
                return array;
            }

            if (jsValue.IsObject && TryGetValueAsObject(jsValue, out object value))
                return value;

            ThrowInvalidObject(jsValue);
            return null;
        }

        private static void ThrowInvalidObject(T jsValue)
        {
            throw new NotSupportedException($"Was requested to extract the value out of a JsValue object but could not figure its type, value={jsValue}, value type == '{jsValue.ValueType}'");
        }

        protected abstract bool TryGetValueAsObject(T jsValue, out object value);
    }

    public class JsPropertyAccessorJint : JsPropertyAccessor<JsHandleJint>
    {
        public JsPropertyAccessorJint(Dictionary<string, CompiledIndexField> groupByFields) : base(groupByFields)
        {
        }

        protected override void AssertTargetType(object target, [CallerMemberName] string caller = null)
        {
            if (target is JsHandleJint jsHandleJint == false)
                throw new ArgumentException(
                    $"{caller} is expecting a target of type of '{nameof(JsHandleJint)}' but got one of type '{target.GetType().Name}'.");


            if (jsHandleJint.Item is ObjectInstance == false)
            {
                throw new ArgumentException($"JsPropertyAccessor.GetPropertiesInOrder is expecting a target assignable from 'IObjectInstance' but got one of type '{target.GetType().Name}' with interfaces: {string.Join(",", target.GetType().GetInterfaces().ToList())}");
            }
        }

        protected override bool TryGetValueAsObject(JsHandleJint jsValue, out object value)
        {
            //object wrapper is an object so it have to come before the object
            if (jsValue.Item is ObjectWrapper wrapper)
            {
                var target = wrapper.Target;
                value = Utils.TypeConverter.TryReturnLazyValue(target, jsValue);
                return true;
            }

            value = jsValue.AsObject();
            return true;
        }
    }

    public class JsPropertyAccessorV8 : JsPropertyAccessor<JsHandleV8>
    {
        public JsPropertyAccessorV8(Dictionary<string, CompiledIndexField> groupByFields) : base(groupByFields)
        {
        }

        protected override void AssertTargetType(object target, [CallerMemberName] string caller = null)
        {
            if (target.GetType() != typeof(JsHandleV8))
                throw new ArgumentException(
                    $"{caller} is expecting a target of type of '{nameof(JsHandleV8)}' but got one of type '{target.GetType().Name}'.");
        }

        protected override bool TryGetValueAsObject(JsHandleV8 jsValue, out object value)
        {
            object boundObject = jsValue.Item.BoundObject;
            if (boundObject != null)
            {
                //TODO: egor we throw here, in original v8 code we continue, need to check (same as TypeConverter)
                value = Utils.TypeConverter.TryReturnLazyValue(boundObject, jsValue);
                return true;
            }

            value = jsValue.AsObject();
            return true;
        }
    }
}
