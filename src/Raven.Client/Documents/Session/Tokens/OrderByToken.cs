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
            writer.Append(_fieldName);

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

            writer.Append(_descending ? " DESC" : " ASC");
        }
    }
}