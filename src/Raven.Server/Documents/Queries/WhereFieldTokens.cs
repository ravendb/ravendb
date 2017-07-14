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
            return QueryExpression.Extract(queryText, (FieldToken)Field);
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

                switch (propertyDetails.Token)
                {
                    case BlittableJsonToken.Integer:
                        return (propertyDetails.Value.ToString(), ValueTokenType.Long);
                    case BlittableJsonToken.LazyNumber:
                        return (propertyDetails.Value.ToString(), ValueTokenType.Double);
                    case BlittableJsonToken.String:
                    case BlittableJsonToken.CompressedString:
                        return (propertyDetails.Value.ToString(), ValueTokenType.String);
                    case BlittableJsonToken.Boolean:
                        var booleanValue = (bool)propertyDetails.Value;

                        if (booleanValue)
                            return (null, ValueTokenType.True);
                        else
                            return (null, ValueTokenType.False);
                    case BlittableJsonToken.Null:
                        return (null, ValueTokenType.Null);
                    default:
                        throw new ArgumentException($"Unhandled token: {propertyDetails.Token}");
                }
            }

            return (ExtractTokenValue(valueToken, queryText), Type);
        }

        public (List<string>, ValueTokenType) GetValuesAndType(string queryText, BlittableJsonReaderObject queryParameters)
        {
            var values = new List<string>(Values.Count);
            ValueTokenType? valuesType = null;

            foreach (var item in Values)
            {
                string value;
                ValueTokenType type;

                if (Type == ValueTokenType.Parameter)
                {
                    switch (Operator)
                    {
                        case OperatorType.In:
                            throw new NotImplementedException("TODO arek");
                        default:
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