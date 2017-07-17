using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public struct WhereFieldTokens
    {
        public readonly FieldToken Field;
        public readonly ValueTokenType Type;
        public readonly ValueToken SingleValue;
        public readonly List<ValueToken> Values;
        public readonly OperatorType Operator;

        public WhereFieldTokens(FieldToken field, ValueTokenType type, ValueToken singleValue, List<ValueToken> values, OperatorType @operator)
        {
            Field = field;
            Type = type;
            SingleValue = singleValue;
            Values = values;
            Operator = @operator;
        }

        public string ExtractFieldName(string queryText)
        {
            return QueryExpression.Extract(queryText, Field);
        }

        private static string ExtractTokenValue(ValueToken valueToken, string queryText)
        {
            string value;
            switch (valueToken.Type)
            {
                case ValueTokenType.String:
                    value = QueryExpression.Extract(queryText, valueToken.TokenStart + 1, valueToken.TokenLength - 2, valueToken.EscapeChars);
                    break;
                default:
                    value = QueryExpression.Extract(queryText, valueToken);
                    break;
            }
            return value;
        }

        public (string Value, ValueTokenType Type) GetSingleValueAndType(string queryText, BlittableJsonReaderObject queryParameters)
        {
            return ExtractValueAndType(SingleValue, queryText, queryParameters);
        }

        private (string Value, ValueTokenType Type) ExtractValueAndType(ValueToken valueToken, string queryText, BlittableJsonReaderObject queryParameters)
        {
            if (Type == ValueTokenType.Parameter)
            {
                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                var parameterName = QueryExpression.Extract(queryText, valueToken);

                var index = queryParameters.GetPropertyIndex(parameterName);

                queryParameters.GetPropertyByIndex(index, ref propertyDetails);

                return GetValueAndTypeFromJson(propertyDetails);
            }

            return (ExtractTokenValue(valueToken, queryText), Type);
        }

        private (List<string>, ValueTokenType) ExtractInParameters(ValueToken valueToken, string queryText, BlittableJsonReaderObject queryParameters)
        {
            Debug.Assert(Type == ValueTokenType.Parameter);

            var property = new BlittableJsonReaderObject.PropertyDetails();

            var parameterName = QueryExpression.Extract(queryText, valueToken);

            var index = queryParameters.GetPropertyIndex(parameterName);

            queryParameters.GetPropertyByIndex(index, ref property);

            switch (property.Token)
            {
                case BlittableJsonToken.StartArray:
                case BlittableJsonToken.StartArray | BlittableJsonToken.OffsetSizeByte:
                case BlittableJsonToken.StartArray | BlittableJsonToken.OffsetSizeShort:
                    if (Operator != OperatorType.In)
                        throw new InvalidOperationException("Array parameter is supported only as a parameter of IN operator");

                    return GetFlatValuesFromJsonArray((BlittableJsonReaderArray)property.Value);
                default:
                    var valueType = GetValueAndTypeFromJson(property);
                    return (new List<string>{valueType.Value}, valueType.Type);
            }
        }

        private static (string Value, ValueTokenType Type) GetValueAndTypeFromJson(BlittableJsonReaderObject.PropertyDetails property)
        {
            switch (property.Token)
            {
                case BlittableJsonToken.Integer:
                    return (property.Value.ToString(), ValueTokenType.Long);
                case BlittableJsonToken.LazyNumber:
                    return (property.Value.ToString(), ValueTokenType.Double);
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                    return (property.Value.ToString(), ValueTokenType.String);
                case BlittableJsonToken.Boolean:
                    var booleanValue = (bool)property.Value;

                    if (booleanValue)
                        return (null, ValueTokenType.True);
                    else
                        return (null, ValueTokenType.False);
                case BlittableJsonToken.Null:
                    return (null, ValueTokenType.Null);
                default:
                    throw new ArgumentException($"Unhandled token: {property.Token}");
            }
        }

        private (List<string> Values, ValueTokenType Type) GetFlatValuesFromJsonArray(BlittableJsonReaderArray array)
        {
            var values = new List<string>(array.Length);

            ValueTokenType? type = null;

            for (int i = 0; i < array.Length; i++)
            {
                var item = array[i];
                ValueTokenType itemType;

                if (item is long)
                {
                    itemType = ValueTokenType.Long;
                    values.Add(item.ToString());
                }
                else if (item is LazyNumberValue)
                {
                    itemType = ValueTokenType.Double;
                    values.Add(item.ToString());
                }
                else if (item is LazyStringValue || item is LazyCompressedStringValue)
                {
                    itemType = ValueTokenType.String;
                    values.Add(item.ToString());
                }
                else if (item is bool boolValue)
                {
                    itemType =boolValue ? ValueTokenType.True : ValueTokenType.False;
                    values.Add(item.ToString());
                }
                else if (item is null)
                {
                    itemType = ValueTokenType.String;
                    values.Add(null);
                }
                else if (item is BlittableJsonReaderArray nestedArray)
                {
                    var (arrayItems, arrayItemsType) = GetFlatValuesFromJsonArray(nestedArray);
                    itemType = arrayItemsType;
                    values.AddRange(arrayItems);
                }
                else
                    throw new ArgumentException($"Unhandled array value type: {item.GetType().FullName}");

                if (type != null && type.Value != itemType)
                    throw new ArgumentException("ThrowIncompatibleTypesOfVariables"); // TODO arek

                type = itemType;
            }

            Debug.Assert(type != null);

            return (values, type.Value);
        }

        public (List<string>, ValueTokenType) GetValuesAndType(string queryText, BlittableJsonReaderObject queryParameters)
        {
            var values = new List<string>(Values.Count);
            ValueTokenType? valuesType = null;

            foreach (var item in Values)
            {
                ValueTokenType type;

                if (Type == ValueTokenType.Parameter)
                {
                    switch (Operator)
                    {
                        case OperatorType.In:
                            var (inValues, inValuesType) = ExtractInParameters(item, queryText, queryParameters);
                            values.AddRange(inValues);
                            type = inValuesType;
                            break;
                        default:
                            string value;
                            (value, type) = ExtractValueAndType(item, queryText, queryParameters);
                            values.Add(value);
                            break;
                    }
                }
                else
                {
                    values.Add(ExtractTokenValue(item, queryText));
                    type = item.Type;
                }

                // TODO
                //if (valuesType != null && valuesType.Value != type)
                //{
                //    if (Type == ValueTokenType.Parameter)
                //        ThrowIncompatibleTypesInQueryParameters(fieldName, whereField.Values.Select(x => GetParameterValueAndType(x, ref propertyDetails)));
                //    else
                //        ThrowIncompatibleTypesOfVariables(fieldName, whereField.Values.Select(x => (ExtractTokenValue(x), x.Type)));
                //}

                valuesType = type;
            }

            Debug.Assert(valuesType != null);

            return (values, valuesType.Value);
        }
    }
}