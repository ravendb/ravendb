using System.Text;
using Raven.Client.Documents.Queries;

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

            if (_ordering != OrderingType.String)
                writer.Append($" AS {_ordering.ToString().ToLowerInvariant()}");

            writer.Append(_descending ? " DESC" : " ASC");
        }

        public override QueryToken Clone()
        {
            return this;
        }
    }
}