using System.Text;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session.Tokens
{
    public class OrderByToken : QueryToken
    {
        private readonly string _fieldName;
        private readonly bool _descending;

        private OrderByToken(string fieldName, bool descending)
        {
            _fieldName = fieldName;
            _descending = descending;
        }

        public static OrderByToken CreateAscending(string fieldName)
        {
            return new OrderByToken(fieldName, descending: false);
        }

        public static OrderByToken CreateDescending(string fieldName)
        {
            return new OrderByToken(fieldName, descending: true);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append(RavenQuery.EscapeField(_fieldName))
                .Append(_descending ? " DESC" : " ASC");
        }

        public override QueryToken Clone()
        {
            return this;
        }
    }
}