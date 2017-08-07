using Jint.Native.Object;
using System;
using System.Collections.Generic;
using Jint;
using Jint.Runtime.Descriptors;
using Sparrow.Json;
using Jint.Native.Function;
using Jint.Native;
using Jint.Native.Array;
using Jint.Runtime;


namespace Raven.Server.Documents.Patch
{
    public class BlittableObjectInstance : ObjectInstance
    {
        public readonly BlittableJsonReaderObject Blittable;
        public Dictionary<string, (bool isDeleted, JsValue value)> Modifications;

        public BlittableObjectInstance(Engine engine, BlittableJsonReaderObject parent) : base(engine)
        {
            Blittable = parent;
        }

        public static JsValue CreateArrayInstanceBasedOnBlittableArray(Engine engine, BlittableJsonReaderArray blittableArray)
        {
            JsValue returnedValue = engine.Array.Construct(Arguments.Empty);
            var valueAsArrayInstance = returnedValue.TryCast<ArrayInstance>();
                        
            for (var i = 0; i < blittableArray.Length; i++)
            {
                var indexAsString = i.ToString();
                BlittableArrayPropertyDescriptor blittablePropertyDescriptor
                    = new BlittableArrayPropertyDescriptor(engine, blittableArray, i);
                valueAsArrayInstance.DefineOwnProperty(indexAsString, blittablePropertyDescriptor, true);
            }

            return returnedValue;
        }

        public override PropertyDescriptor GetOwnProperty(string propertyName)
        {
            if (Properties.TryGetValue(propertyName, out PropertyDescriptor descriptor) == false)
            {
                descriptor = new BlittablePropertyDescriptor(Engine, this, propertyName);
                Properties[propertyName] = descriptor;
            }
            return descriptor;
        }

        public override void RemoveOwnProperty(string p)
        {
            Modifications[p] = (true, null);
            base.RemoveOwnProperty(p);
        }
        
        public class BlittablePropertyDescriptor : PropertyDescriptor
        {
            private Engine _engine;
            public readonly BlittableObjectInstance Self;
            private string _name;
            private JsValue LastKnownValue;

            public BlittablePropertyDescriptor(Engine engine, BlittableObjectInstance self, string name)
            {
                _engine = engine;
                Self = self;
                _name = name;

                Get = new BlittableGetterFunctionInstance(engine, this, name);
                Set = new BlittableSetterFunctionInstance(engine, this, name);
                Writable = true;
                Configurable = true;
            }

            public override JsValue Value
            {
                get
                {
                    LastKnownValue = GetValue();
                    return LastKnownValue;
                }
                set
                {
                    SetValue(value);
                }
            }

            private JsValue GetValue()
            {
                if (LastKnownValue != null)
                    return LastKnownValue;

                if (Self.Modifications != null && Self.Modifications.TryGetValue(_name, out var valTuple))
                {
                    if (valTuple.isDeleted)
                        return JsValue.Undefined;
                    return valTuple.value;
                }

                var propertyIndex = Self.Blittable.GetPropertyIndex(_name);
                if (propertyIndex == -1)
                    return JsValue.Undefined;

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                Self.Blittable.GetPropertyByIndex(propertyIndex, ref propertyDetails, true);

                JsValue returnedValue;
                switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        returnedValue = JsValue.Null;
                        break;
                    case BlittableJsonToken.Boolean:
                        returnedValue = new JsValue((bool)propertyDetails.Value);
                        break;
                    case BlittableJsonToken.Integer:
                        returnedValue = new JsValue((long)propertyDetails.Value);
                        break;
                    case BlittableJsonToken.LazyNumber:
                        returnedValue = new JsValue((double)(LazyNumberValue)propertyDetails.Value);
                        break;
                    case BlittableJsonToken.String:
                        returnedValue = new JsValue(((LazyStringValue)propertyDetails.Value).ToString());
                        break;
                    case BlittableJsonToken.CompressedString:
                        returnedValue = new JsValue(((LazyCompressedStringValue)propertyDetails.Value).ToString());
                        break;
                    case BlittableJsonToken.StartObject:
                        returnedValue = new BlittableObjectInstance(_engine, (BlittableJsonReaderObject)propertyDetails.Value);
                        break;
                    case BlittableJsonToken.StartArray:
                        Enumerable = true; // todo: maybe this should be set earlier

                        //returnedValue = new BlittableObjectArrayInstance(_engine, (BlittableJsonReaderArray)propertyDetails.Value);

                        returnedValue = BlittableObjectInstance.CreateArrayInstanceBasedOnBlittableArray(_engine,propertyDetails.Value as BlittableJsonReaderArray);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(propertyDetails.Token.ToString());
                }

                if (Self.Modifications == null)
                    Self.Modifications = new Dictionary<string, (bool isDeleted, JsValue value)>();

                Self.Modifications[_name] = (false, returnedValue);
                return returnedValue;
            }

            private void SetValue(JsValue newVal)
            {
                if (Self.Modifications == null)
                    Self.Modifications = new Dictionary<string, (bool, JsValue)>();

                //BlittableOjectInstanceOperationScope.ToBlittableValue(newVal, string.Empty, true, token, originalValue)
                // todo: not sure that string.Empty here works fine
                LastKnownValue = newVal;
                Enumerable = newVal.IsArray() || newVal.IsObject();
                Self.Modifications[_name] = (false, newVal);
            }

            public class BlittableGetterFunctionInstance : FunctionInstance
            {
                private BlittablePropertyDescriptor _descriptor;
                private string _name;

                public BlittableGetterFunctionInstance(Engine engine, BlittablePropertyDescriptor descriptor, string name) : base(engine, null, null, false)
                {
                    this._descriptor = descriptor;
                    this._name = name;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    return _descriptor.GetValue();
                }


            }

            public class BlittableSetterFunctionInstance : FunctionInstance
            {
                private BlittablePropertyDescriptor _descriptor;
                private string _name;

                public BlittableSetterFunctionInstance(Engine engine, BlittablePropertyDescriptor descriptor, string name) : base(engine, null, null, false)
                {
                    this._descriptor = descriptor;
                    this._name = name;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    var newVal = arguments[0];
                    _descriptor.SetValue(newVal);

                    return Null.Instance;
                }
            }
        }
    }

    public class BlittableArrayPropertyDescriptor : PropertyDescriptor
    {
        private Engine _engine;
        private int _index;
        public JsValue LastKnownValue { get; set; }
        private BlittableJsonReaderArray _parent;

        public BlittableArrayPropertyDescriptor(Engine engine, BlittableJsonReaderArray parent, int index)
        {
            _engine = engine;
            _index = index;
            _parent = parent;
            // todo: cleanup code here, pretty sure we won't need the _get and _set fields                
            Get = new BlittableGetterFunctionInstance(engine, this, index);
            Set = new BlittableSetterFunctionInstance(engine, this, index);
            this.Writable = true;
            this.Configurable = true;
        }

        public BlittableArrayPropertyDescriptor(Engine engine, BlittableJsonReaderArray parent, int index, JsValue value, bool? writable, bool? enumerable, bool? configurable) : base(value, writable, enumerable, configurable)
        {
            _parent = parent;
            Get = new BlittableGetterFunctionInstance(engine, this, index);
            Set = new BlittableSetterFunctionInstance(engine, this, index);
            this.Writable = true;
        }

        public override JsValue Value
        {
            get
            {
                LastKnownValue = GetValue();
                return LastKnownValue;
            }
            set
            {
                SetValue(value);
            }
        }

        private JsValue GetValue()
        {
            if (LastKnownValue != null)
                return LastKnownValue;

            var valueTuple = _parent.GetValueTokenTupleByIndex(_index);

            switch (valueTuple.Item2 & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    return JsValue.Null;
                case BlittableJsonToken.Boolean:
                    return new JsValue((bool)valueTuple.Item1);

                case BlittableJsonToken.Integer:
                    return new JsValue((long)valueTuple.Item1);
                case BlittableJsonToken.LazyNumber:
                    return new JsValue((double)(LazyNumberValue)valueTuple.Item1);
                case BlittableJsonToken.String:
                    return new JsValue(((LazyStringValue)valueTuple.Item1).ToString());
                case BlittableJsonToken.CompressedString:
                    return new JsValue(((LazyCompressedStringValue)valueTuple.Item1).ToString());

                case BlittableJsonToken.StartObject:
                    return new BlittableObjectInstance(_engine, (BlittableJsonReaderObject)valueTuple.Item1);
                case BlittableJsonToken.StartArray:
                    Enumerable = true;

                    //return new BlittableObjectArrayInstance(_engine, (BlittableJsonReaderArray)valueTuple.Item1);
                    return BlittableObjectInstance.CreateArrayInstanceBasedOnBlittableArray(_engine, valueTuple.Item1 as BlittableJsonReaderArray);
                default:
                    return JsValue.Undefined;
            }
        }

        public void SetValue(JsValue newVal)
        {
            LastKnownValue = newVal;
        }

        public class BlittableGetterFunctionInstance : FunctionInstance
        {
            private int _index;
            private BlittableArrayPropertyDescriptor _descriptor;

            public BlittableGetterFunctionInstance(Engine engine, BlittableArrayPropertyDescriptor descriptor, int index) : base(engine, null, null, false)
            {
                this._index = index;
                _descriptor = descriptor;
            }

            public override JsValue Call(JsValue thisObject, JsValue[] arguments)
            {
                if (_index == -1)
                    return JsValue.Undefined;
                return _descriptor.GetValue();
            }
        }

        public class BlittableSetterFunctionInstance : FunctionInstance
        {
            private int _index;
            private BlittableArrayPropertyDescriptor _descriptor;

            public BlittableSetterFunctionInstance(Engine engine, BlittableArrayPropertyDescriptor descriptor, int index) : base(engine, null, null, false)
            {
                _index = index;
                _descriptor = descriptor;
            }

            public override JsValue Call(JsValue thisObject, JsValue[] arguments)
            {
                var newVal = arguments[0];
                _descriptor.SetValue(newVal);
                return Null.Instance;
            }


        }
    }
}
