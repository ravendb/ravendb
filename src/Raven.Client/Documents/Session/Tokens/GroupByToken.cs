using System.Text;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session.Tokens
{
    public class GroupByToken : QueryToken
    {
        private readonly string _fieldName;

        private GroupByToken(string fieldName)
        {
            _fieldName = fieldName;
        }

        public static GroupByToken Create(string fieldName)
        {
            return new GroupByToken(fieldName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append(_fieldName);
        }
    }
}