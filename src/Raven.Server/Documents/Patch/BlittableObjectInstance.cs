using System;
using System.Collections.Generic;
using Jurassic;
using Jurassic.Library;
using Sparrow.Json;


namespace Raven.Server.Documents.Patch
{
    public class BlittableObjectInstance : ObjectInstance
    {
        public readonly BlittableJsonReaderObject Blittable;
        public HashSet<string> Deletes = new HashSet<string>();
        public HashSet<object> Nested = new HashSet<object>();
                
        public BlittableObjectInstance(ScriptEngine engine, BlittableJsonReaderObject parent) : base(engine)
        {
            Blittable = parent;
        }

        public override bool Delete(object key, bool throwOnError)
        {
            Deletes.Add(key.ToString());
            return base.Delete(key, throwOnError);
        }

        protected override object GetMissingPropertyValue(object key)
        {
            var keyAsString = key.ToString();
            Deletes.Remove(keyAsString);
            
            int propertyIndex = Blittable.GetPropertyIndex(keyAsString);
            if (propertyIndex == -1)
            {
                return base.GetMissingPropertyValue(key);
            }

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

            Blittable.GetPropertyByIndex(propertyIndex, ref propertyDetails, true);

            object returnedValue;
            switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    returnedValue = Null.Value;
                    break;
                case BlittableJsonToken.Boolean:
                    returnedValue = (bool)propertyDetails.Value;
                    break;
                case BlittableJsonToken.Integer:
                    returnedValue = (long)propertyDetails.Value;
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
                    returnedValue = new BlittableObjectInstance(Engine, (BlittableJsonReaderObject)propertyDetails.Value);
                    break;
                case BlittableJsonToken.StartArray:                                        
                    returnedValue = CreateArrayInstanceBasedOnBlittableArray(Engine, propertyDetails.Value as BlittableJsonReaderArray);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(propertyDetails.Token.ToString());
            }

            return returnedValue;
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
                        arrayValue = (long)valueTuple.Item1;
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
                        arrayValue = new BlittableObjectInstance(engine, (BlittableJsonReaderObject)valueTuple.Item1);
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
