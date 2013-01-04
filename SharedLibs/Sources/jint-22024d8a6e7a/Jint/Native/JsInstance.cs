using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;

namespace Jint.Native {
    /// <summary>
    /// A base class for values in javascript.
    /// </summary>
    [Serializable]
    public abstract class JsInstance : IComparable<JsInstance> {
        public static JsInstance[] EMPTY = new JsInstance[0];

        public abstract bool IsClr { get; }

        public abstract object Value { get; set; }

        public PropertyAttributes Attributes { get; set; }

        public JsInstance() {
        }

        public virtual JsInstance ToPrimitive(IGlobal global) {
            return JsUndefined.Instance;
        }

        public virtual bool ToBoolean() {
            return true;
        }

        public virtual double ToNumber() {
            return 0;
        }

        public virtual int ToInteger() {
            return (int)ToNumber();
        }

        public virtual object ToObject() {
            return Value;
        }

        public virtual string ToSource() {
            return ToString();
        }

        public override string ToString() {
            return (Value ?? Class).ToString();
        }

        public override int GetHashCode() {
            return Value != null ? Value.GetHashCode() : base.GetHashCode();
        }

        public const string TYPE_OBJECT = "object";
        public const string TYPE_BOOLEAN = "boolean";
        public const string TYPE_STRING = "string";
        public const string TYPE_NUMBER = "number";
        public const string TYPE_UNDEFINED = "undefined";
        public const string TYPE_NULL = "null";

        public const string TYPE_DESCRIPTOR = "descriptor";

        public const string TYPEOF_FUNCTION = "function"; // used only in typeof operator!!!

        // embed classes ecma262.3 15
        
        public const string CLASS_NUMBER = "Number";
        public const string CLASS_STRING = "String";
        public const string CLASS_BOOLEAN = "Boolean";

        public const string CLASS_OBJECT = "Object";
        public const string CLASS_FUNCTION = "Function";
        public const string CLASS_ARRAY = "Array";
        public const string CLASS_REGEXP = "RegExp";
        public const string CLASS_DATE = "Date";
        public const string CLASS_ERROR = "Error";

        public const string CLASS_ARGUMENTS = "Arguments";
        public const string CLASS_GLOBAL = "Global";
        public const string CLASS_DESCRIPTOR = "Descriptor";
        public const string CLASS_SCOPE = "Scope";

        public const string CLASS_UNDEFINED = "Undefined";
        public const string CLASS_NULL = "Null";

        /// <summary>
        /// Class of an object, don't confuse with type of an object.
        /// </summary>
        /// <remarks>There are only six object types in the ecma262.3: Undefined, Null, Boolean, String, Number, Object</remarks>
        public abstract string Class { get; }

        /// <summary>
        /// A type of a JsObject
        /// </summary>
        public abstract string Type { get; }

        /// <summary>
        /// This is a shortcut to a function call by name.
        /// </summary>
        /// <remarks>
        /// Since this method requires a visitor it's not a very usefull, so this method is deprecated.
        /// </remarks>
        /// <param name="visitor"></param>
        /// <param name="function"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        [Obsolete("will be removed in the 1.0 version",true)]
        public virtual object Call(IJintVisitor visitor, string function, params JsInstance[] parameters) {
            if (function == "toString")
                return visitor.Global.StringClass.New(ToString());
            return JsUndefined.Instance;
        }

        // 11.9.6 The Strict Equality Comparison Algorithm
        public static JsInstance StrictlyEquals(IGlobal global, JsInstance left, JsInstance right)
        {
            if (left.Type != right.Type)
            {
                return global.BooleanClass.False;
            }
            else if (left is JsUndefined)
            {
                return global.BooleanClass.True;
            }
            else if (left is JsNull)
            {
                return global.BooleanClass.True;
            }
            else if (left.Type == JsInstance.TYPE_NUMBER)
            {
                if (left == global.NumberClass["NaN"] || right == global.NumberClass["NaN"])
                {
                    return global.BooleanClass.False;
                }
                else if (left.ToNumber() == right.ToNumber())
                {
                    return global.BooleanClass.True;
                }
                else
                    return global.BooleanClass.False;
            }
            else if (left.Type == JsInstance.TYPE_STRING)
            {
                return global.BooleanClass.New(left.ToString() == right.ToString());
            }
            else if (left.Type == JsInstance.TYPE_BOOLEAN)
            {
                return global.BooleanClass.New(left.ToBoolean() == right.ToBoolean());
            }
            else if (left == right)
            {
                return global.BooleanClass.True;
            }
            else
            {
                return global.BooleanClass.False;
            }            
        }


        #region IComparable<JsInstance> Members

        public int CompareTo(JsInstance other)
        {
            return ToString().CompareTo(other.ToString());
        }

        #endregion
    }
}
