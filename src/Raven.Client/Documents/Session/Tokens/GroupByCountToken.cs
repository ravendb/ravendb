using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class GroupByCountToken : QueryToken
    {
        private readonly string _fieldName;

        private GroupByCountToken(string fieldName)
        {
            _fieldName = fieldName;
        }

        public static GroupByCountToken Create(string fieldName)
        {
            return new GroupByCountToken(fieldName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("count()");

            if (_fieldName == null)
                return;

            writer
                .Append(" as ")
                .Append(_fieldName);
        }

        public override QueryToken Clone()
        {
            return this;
        }
    }
}