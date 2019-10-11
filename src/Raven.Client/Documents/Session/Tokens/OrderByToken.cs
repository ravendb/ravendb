using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class OrderByToken : QueryToken
    {
        private readonly string _fieldName;
        private readonly bool _descending;
        private readonly string _sorterName;
        private readonly OrderingType _ordering;

        private OrderByToken(string fieldName, bool descending, string sorterName)
        {
            _fieldName = fieldName;
            _descending = descending;
            _sorterName = sorterName;
        }

        private OrderByToken(string fieldName, bool descending, OrderingType ordering)
        {
            _fieldName = fieldName;
            _descending = descending;
            _ordering = ordering;
        }

        public static OrderByToken Random = new OrderByToken("random()", descending: false, ordering: OrderingType.String);

        public static OrderByToken ScoreAscending = new OrderByToken("score()", descending: false, ordering: OrderingType.String);

        public static OrderByToken ScoreDescending = new OrderByToken("score()", descending: true, ordering: OrderingType.String);

        public static OrderByToken CreateDistanceAscending(string fieldName, string latitudeParameterName, string longitudeParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, spatial.point(${latitudeParameterName}, ${longitudeParameterName}))", false, OrderingType.String);
        }

        public static OrderByToken CreateDistanceAscending(string fieldName, string shapeWktParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, spatial.wkt(${shapeWktParameterName}))", false, OrderingType.String);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string latitudeParameterName, string longitudeParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, spatial.point(${latitudeParameterName}, ${longitudeParameterName}))", true, OrderingType.String);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string shapeWktParameterName)
        {
            return new OrderByToken($"spatial.distance({fieldName}, spatial.wkt(${shapeWktParameterName}))", true, OrderingType.String);
        }

        public static OrderByToken CreateRandom(string seed)
        {
            if (seed == null)
                throw new ArgumentNullException(nameof(seed));

            return new OrderByToken("random('" + seed.Replace("'", "''") + "')", false, OrderingType.String);
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
    }
}
