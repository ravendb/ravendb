using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Extensions;
using Sparrow.Json;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions.Jint;
using V8.Net;
using LuceneDocumentsJint = Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Jint;
using LuceneDocumentsV8 = Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.V8;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public delegate object DynamicGetter(object target);

    public class PropertyAccessor : IPropertyAccessor
    {
        protected readonly Dictionary<string, Accessor> Properties = new Dictionary<string, Accessor>();

        protected readonly List<KeyValuePair<string, Accessor>> _propertiesInOrder =
            new List<KeyValuePair<string, Accessor>>();

        public static IPropertyAccessor Create(Type type, object instance)
        {
            if (type == typeof(JsHandle) || type == typeof(ObjectInstance) || type == typeof(InternalHandle))
                return new JsPropertyAccessor(null);

            if (instance is Dictionary<string, object> dict)
                return DictionaryAccessor.Create(dict);

            return new PropertyAccessor(type);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IPropertyAccessor CreateMapReduceOutputAccessorJs(Dictionary<string, CompiledIndexField> groupByFields)
        {
            return new JsPropertyAccessor(groupByFields);
        }

        internal static IPropertyAccessor CreateMapReduceOutputAccessor(object instance, Dictionary<string, CompiledIndexField> groupByFields, bool isObjectInstance = false, [CanBeNull] Type type = null)
        {
            if (instance is JsHandle)
                return CreateMapReduceOutputAccessorJs(groupByFields);
            
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

    public class JsPropertyAccessor : IPropertyAccessor
    {
        protected readonly Dictionary<string, CompiledIndexField> _groupByFields;

        public JsPropertyAccessor(Dictionary<string, CompiledIndexField> groupByFields)
        {
            _groupByFields = groupByFields;
        }
        
        public IEnumerable<(string Key, object Value, CompiledIndexField GroupByField, bool IsGroupByField)> GetPropertiesInOrder(object target)
        {

            if (target is JsHandle jsTarget && jsTarget.IsObject)
            {
                foreach (var (propertyName, jsPropertyValue) in jsTarget.GetOwnProperties())
                {
                    using (jsPropertyValue)
                    {
                        CompiledIndexField field = null;
                        var isGroupByField = _groupByFields?.TryGetValue(propertyName, out field) ?? false;

                        yield return (propertyName, GetValue(jsPropertyValue), field, isGroupByField);
                    }
                }
            }
            else
                throw new ArgumentException($"JsPropertyAccessor.GetPropertiesInOrder is expecting a target of type JsHandle and IsObject but got one of type {target.GetType().Name}.");
        }

        public object GetValue(string name, object target)
        {
            if (target is ObjectInstance oi)
                return GetValueJint(name, oi);
            else if (target is InternalHandle ih)
                return GetValueV8(name, ih);
            else
                throw new ArgumentException($"JsPropertyAccessor.GetValue is expecting a target of type ObjectInstance or InternalHandle but got one of type {target.GetType().Name}.");
        }

        private static object GetValueJint(string name, ObjectInstance jsValue)
        {
            if (jsValue.HasOwnProperty(name) == false)
                throw new MissingFieldException($"The target for 'JsPropertyAccessor.GetValue' doesn't contain the property {name}.");
            return GetValue(jsValue.GetProperty(name).Value);
        }

        private static object GetValueV8(string name, InternalHandle jsValue)
        {
            if (jsValue.HasOwnProperty(name))
                throw new MissingFieldException($"The target for 'JsPropertyAccessor.GetValue' doesn't contain the property {name}.");
            using (var jsProp = jsValue.GetProperty(name))
            {
                return GetValue(jsProp);
            }
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
                ThrowInvalidObject(new JsHandle(jsValue));
            }
            else if (jsValue.IsArray())
            {
                var arr = jsValue.AsArray();
                var array = new object[arr.Length];
                var i = 0;
                foreach ((var key, var val) in arr.GetOwnPropertiesWithoutLength())
                {
                    array[i++] = GetValue(val.Value);
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

            ThrowInvalidObject(new JsHandle(jsValue));
            return null;
        }
        
        private static object GetValue(InternalHandle jsValue)
        {
            if (jsValue.IsNull)
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
                return jsValue.AsDate;

            if (jsValue.IsArray)
            {
                int arrayLength =  jsValue.ArrayLength;
                var array = new object[arrayLength];
                for (int i = 0; i < arrayLength; i++)
                {
                    using (var jsItem = jsValue.GetProperty(i))
                        array[i] = GetValue(jsItem);
                }
                return array;
            }

            if (jsValue.IsObject)
            {
                var boundObject = jsValue.BoundObject;
                if (boundObject != null)
                {
                    switch (boundObject)
                    {
                        case LazyStringValue lsv:
                            return lsv;

                        case LazyCompressedStringValue lcsv:
                            return lcsv;

                        case LazyNumberValue lnv:
                            return lnv; //should be already blittable supported type.
                    }
                }
                return new InternalHandle(ref jsValue, true);
            }

            if (jsValue.IsUndefined)
            {
                return null;
            }

            ThrowInvalidObject(new JsHandle(jsValue));
            return null;
        }

        private static object GetValue(JsHandle jsValue)
        {
            var engineType = jsValue.EngineType;
            return engineType switch
            {
                JavaScriptEngineType.Jint => GetValue(jsValue.Jint.Item),
                JavaScriptEngineType.V8 => GetValue(jsValue.V8.Item),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{engineType}'.")
            };
        }

        private static object GetValueUnified(JsHandle jhValue) // we can replace the above three methods with this one but at some cost to performance
        {
            var jsValue = jhValue.Handler; // this is with boxing, without boxing but with two step access on each call would be if not using handler: jsValue = jhValue
            if (jsValue.IsNull)
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
                return jsValue.AsDate;

            if (jsValue.IsArray)
            {
                var arrayLength =  jsValue.ArrayLength;
                var array = new object[arrayLength];
                for (int i = 0; i < arrayLength; i++)
                {
                    using (var jsItem = jsValue.GetProperty(i))
                        array[i] = GetValue(jsItem);
                }
                return array;
            }

            if (jsValue.IsObject)
            {
                var boundObject = jsValue.Object;
                if (boundObject != null)
                {
                    switch (boundObject)
                    {
                        case LazyStringValue lsv:
                            return lsv;

                        case LazyCompressedStringValue lcsv:
                            return lcsv;

                        case LazyNumberValue lnv:
                            return lnv; //should be already blittable supported type.
                    }
                }
                return jhValue.Clone();
            }

            if (jsValue.IsUndefined)
            {
                return null;
            }

            ThrowInvalidObject(jhValue);
            return null;
        }

        private static void ThrowInvalidObject(JsHandle jsValue)
        {
            throw new NotSupportedException($"Was requested to extract the value out of a JsValue object but could not figure its type, value={jsValue.ValueType}");
        }
    }
}
