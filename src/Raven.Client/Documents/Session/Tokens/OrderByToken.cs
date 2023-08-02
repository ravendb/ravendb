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
        private readonly bool _isMethodField;

        private OrderByToken(string fieldName, bool descending, string sorterName, bool isMethodField = false)
        {
            _fieldName = fieldName;
            _descending = descending;
            _sorterName = sorterName;
            _isMethodField = isMethodField;
        }

        private OrderByToken(string fieldName, bool descending, OrderingType ordering, bool isMethodField = false)
        {
            _fieldName = fieldName;
            _descending = descending;
            _ordering = ordering;
            _isMethodField = isMethodField;
        }

        public static OrderByToken Random = new OrderByToken("random()", descending: false, ordering: OrderingType.String, isMethodField: true);

        public static OrderByToken ScoreAscending = new OrderByToken("score()", descending: false, ordering: OrderingType.String, isMethodField: true);

        public static OrderByToken ScoreDescending = new OrderByToken("score()", descending: true, ordering: OrderingType.String, isMethodField: true);

        public static OrderByToken CreateDistanceAscending(string fieldName, string latitudeParameterName, string longitudeParameterName, string roundFactorParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, spatial.point(${latitudeParameterName}, " +
                $"${longitudeParameterName}" +
                $"){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})", false, OrderingType.String, isMethodField: true);
        }

        public static OrderByToken CreateDistanceAscending(string fieldName, string shapeWktParameterName, string roundFactorParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, spatial.wkt(${shapeWktParameterName}" +
                $"){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})", false, OrderingType.String, isMethodField: true);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string latitudeParameterName, string longitudeParameterName, string roundFactorParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, " +
                $"spatial.point(${latitudeParameterName}, ${longitudeParameterName}" +
                $"){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})", true, OrderingType.String, isMethodField: true);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string shapeWktParameterName, string roundFactorParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, spatial.wkt(${shapeWktParameterName}" +
                $"){(roundFactorParameterName == null ? "" : ", $" + roundFactorParameterName)})", true, OrderingType.String, isMethodField: true);
        }

        public static OrderByToken CreateRandom(string seed)
        {
            if (seed == null)
                throw new ArgumentNullException(nameof(seed));

            return new OrderByToken("random('" + seed.Replace("'", "''") + "')", false, OrderingType.String, isMethodField: true);
        }

        public static OrderByToken CreateAscending(string fieldName, string sorterName)
        {
            return new OrderByToken(fieldName, false, sorterName);
        }

        public static OrderByToken CreateAscending(string fieldName, OrderingType ordering)
        {
            return new OrderByToken(fieldName, false, ordering);
        }

        public static OrderByToken CreateDescending(string fieldName, string sorterName)
        {
            return new OrderByToken(fieldName, true, sorterName);
        }

        public static OrderByToken CreateDescending(string fieldName, OrderingType ordering)
        {
            return new OrderByToken(fieldName, true, ordering);
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
            else
                return new OrderByToken(aliasedName, _descending, _ordering);
        }
    }
}
