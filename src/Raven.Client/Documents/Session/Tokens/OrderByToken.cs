using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class OrderByToken : QueryToken
    {
        private readonly string _fieldName;
        private readonly bool _descending;
        private readonly string _sorterName;
        private readonly OrderingType _ordering;
        private readonly NullsOrdering _nullsOrdering;
        private readonly bool _isMethodField;

        private OrderByToken(string fieldName, bool descending, string sorterName, bool isMethodField = false)
        {
            _fieldName = fieldName;
            _descending = descending;
            _sorterName = sorterName;
            _isMethodField = isMethodField;
        }

        private OrderByToken(string fieldName, bool descending, OrderingType ordering, NullsOrdering nullsOrdering = NullsOrdering.Default, bool isMethodField = false)
        {
            _fieldName = fieldName;
            _descending = descending;
            _ordering = ordering;
            _nullsOrdering = nullsOrdering;
            _isMethodField = isMethodField;
        }

        public static OrderByToken Random = new OrderByToken("random()", descending: false, ordering: OrderingType.String, isMethodField: true);

        public static OrderByToken ScoreAscending = new OrderByToken("score()", descending: false, ordering: OrderingType.String, isMethodField: true);

        public static OrderByToken ScoreDescending = new OrderByToken("score()", descending: true, ordering: OrderingType.String, isMethodField: true);

        public static OrderByToken CreateDistanceAscending(string fieldName, string latitudeParameterName, string longitudeParameterName, string roundFactorParameterName, NullsOrdering nulls = NullsOrdering.Default)
        {
            return new OrderByToken(
                $"spatial.distance({fieldName}, spatial.point(${latitudeParameterName}, ${longitudeParameterName}){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})",
                descending: false, OrderingType.String, nulls, isMethodField: true);
        }

        public static OrderByToken CreateDistanceAscending(string fieldName, string shapeWktParameterName, string roundFactorParameterName, NullsOrdering nulls = NullsOrdering.Default)
        {
            return new OrderByToken(
                $"spatial.distance({fieldName}, spatial.wkt(${shapeWktParameterName}){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})",
                descending: false, OrderingType.String, nulls, isMethodField: true);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string latitudeParameterName, string longitudeParameterName, string roundFactorParameterName, NullsOrdering nulls = NullsOrdering.Default)
        {
            return new OrderByToken(
                $"spatial.distance({fieldName}, spatial.point(${latitudeParameterName}, ${longitudeParameterName}){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})",
                descending: true, OrderingType.String, nulls, isMethodField: true);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string shapeWktParameterName, string roundFactorParameterName, NullsOrdering nulls = NullsOrdering.Default)
        {
            return new OrderByToken(
                $"spatial.distance({fieldName}, spatial.wkt(${shapeWktParameterName}){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})",
                descending: true, OrderingType.String, nulls, isMethodField: true);
        }

        public static OrderByToken CreateRandom(string seed)
        {
            if (seed == null)
                throw new ArgumentNullException(nameof(seed));

            return new OrderByToken("random('" + seed.Replace("'", "''") + "')", false, OrderingType.String, isMethodField: true);
        }

        public static OrderByToken CreateAscending(string fieldName, string sorterName)
        {
            return new OrderByToken(fieldName, descending: false, sorterName);
        }

        public static OrderByToken CreateDescending(string fieldName, string sorterName)
        {
            return new OrderByToken(fieldName, descending: true, sorterName);
        }

        public static OrderByToken CreateAscending(string fieldName, OrderingType ordering, NullsOrdering nulls = NullsOrdering.Default)
        {
            return new OrderByToken(fieldName, descending: false, ordering, nulls);
        }

        public static OrderByToken CreateDescending(string fieldName, OrderingType ordering, NullsOrdering nulls = NullsOrdering.Default)
        {
            return new OrderByToken(fieldName, descending: true, ordering, nulls);
        }

        public override void WriteTo(StringBuilder writer)
        {
            if (_sorterName != null)
            {
                writer
                    .Append("custom(");
            }

            WriteField(writer, _fieldName);


            if (_sorterName != null)
            {
                writer
                    .Append(", '")
                    .Append(_sorterName)
                    .Append("')");
            }
            else
            {
                switch (_ordering)
                {
                    case OrderingType.Long:
                        writer.Append(" as long");
                        break;
                    case OrderingType.Double:
                        writer.Append(" as double");
                        break;
                    case OrderingType.AlphaNumeric:
                        writer.Append(" as alphaNumeric");
                        break;
                }
            }

            if (_descending) // we only add this if we have to, ASC is the default and reads nicer
                writer.Append(" desc");

            switch (_nullsOrdering)
            {
                case NullsOrdering.First:
                    writer.Append(" nulls first");
                    break;
                case NullsOrdering.Last:
                    writer.Append(" nulls last");
                    break;
            }
        }

        public OrderByToken AddAlias(string alias)
        {
            if (_fieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return this;

            if (_isMethodField) // we must not alias RQL methods
                return this;

            var aliasedName = $"{alias}.{_fieldName}";

            if (_sorterName != null)
                return new OrderByToken(aliasedName, _descending, _sorterName);

            return new OrderByToken(aliasedName, _descending, _ordering, _nullsOrdering);
        }
    }
}
