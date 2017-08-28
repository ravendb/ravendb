using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class OrderByToken : QueryToken
    {
        private readonly string _fieldName;
        private readonly bool _descending;
        private readonly OrderingType _ordering;

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
            return new OrderByToken($"distance({fieldName}, point(${latitudeParameterName}, ${longitudeParameterName}))", false, OrderingType.String);
        }

        public static OrderByToken CreateDistanceAscending(string fieldName, string shapeWktParameterName)
        {
            return new OrderByToken($"distance({fieldName}, wkt(${shapeWktParameterName}))", false, OrderingType.String);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string latitudeParameterName, string longitudeParameterName)
        {
            return new OrderByToken($"distance({fieldName}, point(${latitudeParameterName}, ${longitudeParameterName}))", true, OrderingType.String);
        }

        public static OrderByToken CreateDistanceDescending(string fieldName, string shapeWktParameterName)
        {
            return new OrderByToken($"distance({fieldName}, wkt(${shapeWktParameterName}))", true, OrderingType.String);
        }

        public static OrderByToken CreateRandom(string seed)
        {
            if (seed == null)
                throw new ArgumentNullException(nameof(seed));

            return new OrderByToken("random('" + seed.Replace("'", "''") + "')", false, OrderingType.String);
        }

        public static OrderByToken CreateAscending(string fieldName, OrderingType ordering)
        {
            return new OrderByToken(fieldName, false, ordering);
        }

        public static OrderByToken CreateDescending(string fieldName, OrderingType ordering)
        {
            return new OrderByToken(fieldName, true, ordering);
        }

        public override void WriteTo(StringBuilder writer)
        {
            WriteField(writer, _fieldName);

            switch (_ordering)
            {
                case OrderingType.Long:
                    writer.Append(" AS long");
                    break;
                case OrderingType.Double:
                    writer.Append(" AS double");
                    break;
                case OrderingType.AlphaNumeric:
                    writer.Append(" AS alphaNumeric");
                    break;
            }

            if (_descending) // we only add this if we have to, ASC is the default and reads nicer
                writer.Append(" DESC");
        }
    }
}
