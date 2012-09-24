using System;
using System.Collections.Generic;
using Jint.Native;
using System.Reflection;
using Jint.Marshal;
using System.Text.RegularExpressions;

namespace Jint
{
    /// <summary>
    /// Marshals clr objects to js objects and back. It can marshal types, delegates and other types of objects.
    /// </summary>
    /// <remarks>
    /// <pre>
    /// Marshaller holds a reference to a global object which is used to get a prototype while marshalling from
    /// clr to js. Futhermore a marshaller is to be accessible while running a script, therefore it strictly
    /// linked to the global object which defines a runtime environment for the script.
    /// </pre>
    /// </remarks>
    public class Marshaller
    {

        IGlobal m_global;
        Dictionary<Type, NativeConstructor> m_typeCache = new Dictionary<Type,NativeConstructor>();
        Dictionary<Type, Delegate> m_arrayMarshllers = new Dictionary<Type, Delegate>();
        NativeTypeConstructor m_typeType;

        /* Assuming that Object supports IConvertable
         *
         */
        static bool[,] IntegralTypeConvertions = {
        //      Empty   Object  DBNull  Boolean Char    SByte   Byte    Int16   UInt16  Int32   UInt32  Int64   UInt64  Single  Double  Decimal DateTim -----   String
/*Empty*/   {   false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  true    },
/*Objec*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  true    },
/*DBNul*/   {   false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  true    },
/*Boole*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Char */   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  false,  false,  false,  true    },
/*SByte*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Byte */   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Int16*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*UInt1*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Int32*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*UInt3*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Int64*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*UInt6*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Singl*/   {   false,  false,  false,  true,   false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Doubl*/   {   false,  false,  false,  true,   false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*Decim*/   {   false,  false,  false,  true,   false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  false,  true    },
/*DateT*/   {   false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  true,   false,  true    },
/*-----*/   {   false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false,  false   },
/*Strin*/   {   false,  false,  false,  true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   true,   false,  true    }
        };

        /// <summary>
        /// Constaructs a new marshaller object.
        /// </summary>
        /// <param name="global">A global object which can be used for constructing new JsObjects while marshalling.</param>
        public Marshaller(IGlobal global)
        {
            this.m_global = global;
        }

        public void InitTypes()
        {
            // we cant initize a m_typeType property since m_global.Marshller should be initialized
            m_typeType = NativeTypeConstructor.CreateNativeTypeConstructor(m_global);

            m_typeCache[typeof(Type)] = m_typeType;

            //TODO: replace a native contructors with apropriate js constructors
            foreach (var t in new Type[] {
                typeof(Int16),
                typeof(Int32),
                typeof(Int64),
                typeof(UInt16),
                typeof(UInt32),
                typeof(UInt64),
                typeof(Single),
                typeof(Double), // NumberConstructor
                typeof(Byte),
                typeof(SByte)
            })
                m_typeCache[t] = CreateConstructor(t, m_global.NumberClass.PrototypeProperty);

            m_typeCache[typeof(String)] = CreateConstructor(typeof(String), m_global.StringClass.PrototypeProperty);
            m_typeCache[typeof(Char)] = CreateConstructor(typeof(Char), m_global.StringClass.PrototypeProperty);
            m_typeCache[typeof(Boolean)] = CreateConstructor(typeof(Boolean), m_global.BooleanClass.PrototypeProperty);
            m_typeCache[typeof(DateTime)] = CreateConstructor(typeof(DateTime), m_global.DateClass.PrototypeProperty);
            m_typeCache[typeof(Regex)] = CreateConstructor(typeof(Regex), m_global.RegExpClass.PrototypeProperty);

        }

        /// <summary>
        /// Marshals a native value to a JsInstance
        /// </summary>
        /// <typeparam name="T">A type of a native value</typeparam>
        /// <param name="value">A native value</param>
        /// <returns>A marshalled JsInstance</returns>
        public JsInstance MarshalClrValue<T>(T value)
        {
            if (value == null)
                return JsNull.Instance;

            if (value is JsInstance)
                return value as JsInstance;

            if (value is Type)
            {
                Type t = value as Type;
                if (t.IsGenericTypeDefinition)
                {
                    // Generic defenitions aren't types in the meaning of js
                    // but they are instances of System.Type
                    var res = new NativeGenericType(t, m_typeType.PrototypeProperty);
                    m_typeType.SetupNativeProperties(res);
                    return res;
                }
                else
                {
                    return MarshalType(value as Type);
                }
            }
            else
            {
                return MarshalType(value.GetType()).Wrap(value);
            }
        }

        public JsConstructor MarshalType(Type t)
        {
            NativeConstructor res;
            if (m_typeCache.TryGetValue(t, out res))
                return res;
            
            return m_typeCache[t] = CreateConstructor(t);
        }

        NativeConstructor CreateConstructor(Type t)
        {
            // TODO: Move this code to NativeTypeConstructor.Wrap
            /* NativeConstructor res;
            res = new NativeConstructor(t, m_global);
            res.InitPrototype(m_global);
            m_typeType.SetupNativeProperties(res);
            return res;
            */
            return (NativeConstructor)m_typeType.Wrap(t);
        }

        /// <summary>
        /// Creates a constructor for a native type and sets its 'prototype' property to
        /// the object derived from a <paramref name="prototypePropertyPrototype"/>.
        /// </summary>
        /// <remarks>
        /// For example native strings should be derived from <c>'String'</c> class i.e. they should
        /// contain a <c>String.prototype</c> object in theirs prototype chain.
        /// </remarks>
        /// <param name="t"></param>
        /// <param name="prototypePropertyPrototype"></param>
        /// <returns></returns>
        NativeConstructor CreateConstructor(Type t, JsObject prototypePropertyPrototype)
        {
            /* NativeConstructor res;
            res = new NativeConstructor(t, m_global,prototypeProperty);
            res.InitPrototype(m_global);
            m_typeType.SetupNativeProperties(res);
            return res; */
            return (NativeConstructor)m_typeType.WrapSpecialType(t, prototypePropertyPrototype);
        }

        TElem[] MarshalJsArrayHelper<TElem>(JsObject value)
        {
            int len = (int)value["length"].ToNumber();
            if (len < 0)
                len = 0;

            TElem[] res = new TElem[len];
            for (int i = 0; i < len; i++)
                res[i] = MarshalJsValue<TElem>(value[new JsNumber(i, JsUndefined.Instance)]);

            return res;
        }

        object MarshalJsFunctionHelper(JsFunction func,Type delegateType)
        {
            // create independent visitor
            ExecutionVisitor visitor = new ExecutionVisitor(m_global, new JsScope((JsObject)m_global));
            var v = ((ExecutionVisitor)m_global.Visitor);
            visitor.AllowClr = v.AllowClr;
            visitor.PermissionSet = v.PermissionSet;

            JsFunctionDelegate wrapper = new JsFunctionDelegate(visitor, func, JsNull.Instance , delegateType);
            return wrapper.GetDelegate();
        }

        /// <summary>
        /// Marshals a JsInstance to a native value.
        /// </summary>
        /// <typeparam name="T">A native object type</typeparam>
        /// <param name="value">A JsInstance to marshal</param>
        /// <returns>A converted native velue</returns>
        public T MarshalJsValue<T>(JsInstance value)
        {
            if (value.Value is T)
            {
                return (T)value.Value;
            }
            else
            {
                if (typeof(T).IsArray)
                {
                    if (value == null || value == JsUndefined.Instance || value == JsNull.Instance)
                        return default(T);
                    if (m_global.ArrayClass.HasInstance(value as JsObject))
                    {
                        Delegate marshller;
                        if (!m_arrayMarshllers.TryGetValue(typeof(T), out marshller))
                            m_arrayMarshllers[typeof(T)] = marshller = Delegate.CreateDelegate(
                                typeof(Func<JsObject, T>),
                                this,
                                typeof(Marshaller)
                                    .GetMethod("MarshalJsFunctionHelper")
                                    .MakeGenericMethod(typeof(T).GetElementType())
                            );

                        return ((Func<JsObject, T>)marshller)(value as JsObject);
                    }
                    else
                    {
                        throw new JintException("Array is required");
                    }
                }
                else if (typeof(Delegate).IsAssignableFrom(typeof(T)))
                {
                    if (value == null || value == JsUndefined.Instance || value == JsNull.Instance)
                        return default(T);

                    if (! (value is JsFunction) )
                        throw new JintException("Can't convert a non function object to a delegate type");
                    return (T)MarshalJsFunctionHelper(value as JsFunction, typeof(T));
                }
                else if (value != JsNull.Instance && value != JsUndefined.Instance && value is T)
                {
                    return (T)(object)value;
                }
                else
                {
                    // JsNull and JsUndefined will fall here and become a nulls
                    return (T)Convert.ChangeType(value.Value, typeof(T));
                }
            }
        }

        /// <summary>
        /// This is a temporary solution... Used when calling a members on value types.
        /// </summary>
        /// <remarks>
        /// This method used to get a reference to the boxed value type, then a reference is
        /// unboxed to managed pointer and then is used as the first argument in a method call.
        /// </remarks>
        /// <typeparam name="T">Type of value type, which we desire to get</typeparam>
        /// <param name="value">A js value which should be marshalled</param>
        /// <returns>A reference to a boxed value</returns>
        public object MarshalJsValueBoxed<T>(JsInstance value)
        {
            if (value.Value is T)
                return value.Value;
            else
                return null;
        }

        /// <summary>
        /// Gets a type of a native object represented by the current JsInstance.
        /// If JsInstance is a pure JsObject than returns a type of js object itself.
        /// </summary>
        /// <remarks>
        /// If a value is a wrapper around native value (like String, Number or a marshaled native value)
        /// this method returns a type of a stored value.
        /// If a value is an js object (constructed with a pure js function) this method returns
        /// a type of this value (for example JsArray, JsObject)
        /// </remarks>
        /// <param name="value">JsInstance value</param>
        /// <returns>A Type object</returns>
        public Type GetInstanceType(JsInstance value)
        {
            if (value == null || value == JsUndefined.Instance || value == JsNull.Instance )
                return null;

            if (value.Value != null )
                return value.Value.GetType();

            return value.GetType();
        }

        #region wrappers

        /// <summary>
        /// Converts a native method to a standard delegate.
        /// </summary>
        /// <param name="info">A method to wrap</param>
        /// <param name="passGlobal">If this paramerter is true and the first argument of the constructor
        /// is IGlobal, a wrapper delegate will pass a Global JS object in the first parameter.</param>
        /// <returns>A wrapper delegate</returns>
        public JsMethodImpl WrapMethod(MethodInfo info, bool passGlobal)
        {
            return ProxyHelper.Default.WrapMethod(info, passGlobal);
        }

        /// <summary>
        /// Converts a constructor to a standart delegate
        /// </summary>
        /// <param name="info">A constructor to wrap</param>
        /// <param name="passGlobal">If this paramerter is true and the first argument of the constructor
        /// is IGlobal, a wrapper delegate will pass a Global JS object in the first parameter.</param>
        /// <returns>A wrapper delegate</returns>
        public ConstructorImpl WrapConstructor(ConstructorInfo info, bool passGlobal) {
            return ProxyHelper.Default.WrapConstructor(info, passGlobal);
        }

        public JsGetter WrapGetProperty(PropertyInfo prop) {
            return ProxyHelper.Default.WrapGetProperty(prop,this);
        }

        public JsGetter WrapGetField(FieldInfo field) {
            return ProxyHelper.Default.WrapGetField(field,this);
        }

        public JsSetter WrapSetProperty(PropertyInfo prop) {
            return ProxyHelper.Default.WrapSetProperty(prop, this);
        }

        public JsSetter WrapSetField(FieldInfo field) {
            return ProxyHelper.Default.WrapSetField(field, this);
        }

        public JsIndexerGetter WrapIndexerGetter(MethodInfo getMethod) {
            return ProxyHelper.Default.WrapIndexerGetter(getMethod, this);
        }

        public JsIndexerSetter WrapIndexerSetter(MethodInfo setMethod) {
            return ProxyHelper.Default.WrapIndexerSetter(setMethod, this);
        }

        /// <summary>
        /// Marshals a native property to a descriptor
        /// </summary>
        /// <param name="prop">Property to marshal</param>
        /// <param name="owner">Owner of the returned descriptor</param>
        /// <returns>A descriptor</returns>
        public NativeDescriptor MarshalPropertyInfo(PropertyInfo prop, JsDictionaryObject owner)
        {
            JsGetter getter;
            JsSetter setter = null;

            if (prop.CanRead && prop.GetGetMethod() != null)
            {
                getter = WrapGetProperty(prop);
            }
            else
            {
                getter = delegate(JsDictionaryObject that)
                {
                    return JsUndefined.Instance;
                };
            }

            if (prop.CanWrite && prop.GetSetMethod() != null)
            {
                setter = (JsSetter)WrapSetProperty(prop);
            }

            return setter == null ? new NativeDescriptor(owner, prop.Name, getter) { Enumerable = true } : new NativeDescriptor(owner, prop.Name, getter, setter) { Enumerable = true };
        }

        /// <summary>
        /// Marshals a native field to a JS Descriptor
        /// </summary>
        /// <param name="prop">Field info to marshal</param>
        /// <param name="owner">Owner for the descriptor</param>
        /// <returns>Descriptor</returns>
        public NativeDescriptor MarshalFieldInfo(FieldInfo prop, JsDictionaryObject owner)
        {
            JsGetter getter;
            JsSetter setter;

            if (prop.IsLiteral)
            {
                JsInstance value = null; // this demand initization should prevent a stack overflow while reflecting types
                getter = delegate(JsDictionaryObject that) {
                    if (value == null)
                        value = (JsInstance)typeof(Marshaller)
                            .GetMethod("MarshalClrValue")
                            .MakeGenericMethod(prop.FieldType)
                            .Invoke(this, new object[] { prop.GetValue(null) });
                    return value;
                };
                setter = delegate(JsDictionaryObject that, JsInstance v) { };
            }
            else
            {
                getter = (JsGetter)WrapGetField(prop);
                setter = (JsSetter)WrapSetField(prop);
            }

            return new NativeDescriptor(owner, prop.Name, getter, setter) { Enumerable = true };
        }

        #endregion

        public bool IsAssignable(Type target, Type source)
        {
            return
                (typeof(IConvertible).IsAssignableFrom(source) && IntegralTypeConvertions[(int)Type.GetTypeCode(source), (int)Type.GetTypeCode(target)])
                || target.IsAssignableFrom(source);
        }
    }
}
