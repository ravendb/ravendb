using System;
using System.Collections.Generic;
using Jurassic;
using Jurassic.Library;
using Sparrow.Json;


namespace Raven.Server.Documents.Patch
{
    public class NullObjectInstance : ObjectInstance
    {
        public NullObjectInstance(ObjectInstance prototype) : base(prototype)
        {
        }

        protected override object GetMissingPropertyValue(object key)
        {
            return new NullObjectInstance(Prototype);
        }
    }
    public class BlittableObjectInstance : ObjectInstance
    {
        public readonly BlittableJsonReaderObject Blittable;
        public readonly string DocumentId;
        public HashSet<string> Deletes;

        public BlittableObjectInstance(ScriptEngine engine, BlittableJsonReaderObject parent, string docId) : base(engine)
        {
            Blittable = parent;
            DocumentId = docId;
        }

        public override bool Delete(object key, bool throwOnError)
        {
            if(Deletes == null)
                Deletes = new HashSet<string>();
            Deletes.Add(key.ToString());
            return base.Delete(key, throwOnError);
        }

        protected override object GetMissingPropertyValue(object key)
        {
            var keyAsString = key.ToString();
            Deletes?.Remove(keyAsString);

            int propertyIndex = Blittable.GetPropertyIndex(keyAsString);
            if (propertyIndex == -1)
            {
                return new NullObjectInstance(Prototype);
            }

            var value = GetMissingPropertyValue(propertyIndex);

            this[key] = value;

            return value;
        }

        private object GetMissingPropertyValue(int propertyIndex)
        {
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

            Blittable.GetPropertyByIndex(propertyIndex, ref propertyDetails, true);

            object returnedValue;
            switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    returnedValue = new NullObjectInstance(Prototype);
                    break;
                case BlittableJsonToken.Boolean:
                    returnedValue = (bool)propertyDetails.Value;
                    break;
                case BlittableJsonToken.Integer:
                    returnedValue = GetJurrasicNumber_TEMPORARY(propertyDetails.Value);
                    break;
                case BlittableJsonToken.LazyNumber:
                    returnedValue = (double)(LazyNumberValue)propertyDetails.Value;
                    break;
                case BlittableJsonToken.String:
                    returnedValue = ((LazyStringValue)propertyDetails.Value).ToString();
                    break;
                case BlittableJsonToken.CompressedString:
                    returnedValue = ((LazyCompressedStringValue)propertyDetails.Value).ToString();
                    break;
                case BlittableJsonToken.StartObject:
                    returnedValue = new BlittableObjectInstance(Engine, (BlittableJsonReaderObject)propertyDetails.Value, null);
                    break;
                case BlittableJsonToken.StartArray:
                    returnedValue = CreateArrayInstanceBasedOnBlittableArray(Engine, propertyDetails.Value as BlittableJsonReaderArray);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(propertyDetails.Token.ToString());
            }

            return returnedValue;
        }

        public static object GetJurrasicNumber_TEMPORARY(object val)
        {
            var value = (long)val;
            // TODO: Maxim fix me, Jurrasic doesn't support longs
            // TODO: RavenDB-8263
            if (value < int.MaxValue)
                return (int)value;
            return (double)value;
        }

        public static ArrayInstance CreateArrayInstanceBasedOnBlittableArray(ScriptEngine engine, BlittableJsonReaderArray blittableArray)
        {
            var returnedValue = engine.Array.Construct();

            for (var i = 0; i < blittableArray.Length; i++)
            {
                var valueTuple = blittableArray.GetValueTokenTupleByIndex(i);
                object arrayValue;
                switch (valueTuple.Item2 & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        arrayValue = Null.Value;
                        break;
                    case BlittableJsonToken.Boolean:
                        arrayValue = (bool)valueTuple.Item1;
                        break;
                    case BlittableJsonToken.Integer:
                        arrayValue = GetJurrasicNumber_TEMPORARY(valueTuple.Item1);
                        break;
                    case BlittableJsonToken.LazyNumber:
                        arrayValue = (double)(LazyNumberValue)valueTuple.Item1;
                        break;
                    case BlittableJsonToken.String:
                        arrayValue = ((LazyStringValue)valueTuple.Item1).ToString();
                        break;
                    case BlittableJsonToken.CompressedString:
                        arrayValue = ((LazyCompressedStringValue)valueTuple.Item1).ToString();
                        break;
                    case BlittableJsonToken.StartObject:
                        arrayValue = new BlittableObjectInstance(engine, (BlittableJsonReaderObject)valueTuple.Item1, null);
                        break;
                    case BlittableJsonToken.StartArray:
                        arrayValue = CreateArrayInstanceBasedOnBlittableArray(engine, valueTuple.Item1 as BlittableJsonReaderArray);
                        break;
                    default:
                        arrayValue = Undefined.Value;
                        break;
                }

                returnedValue.Push(arrayValue);
            }

            return returnedValue;
        }
    }
}
