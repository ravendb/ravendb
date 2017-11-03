using System.Text;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session.Tokens
{
    public class GroupByToken : QueryToken
    {
        private readonly string _fieldName;
        private readonly GroupByMethod _method;

        private GroupByToken(string fieldName, GroupByMethod method)
        {
            _fieldName = fieldName;
            _method = method;
        }

        public static GroupByToken Create(string fieldName, GroupByMethod method = GroupByMethod.None)
        {
            return new GroupByToken(fieldName, method);
        }

        public override void WriteTo(StringBuilder writer)
        {
            if (_method != GroupByMethod.None)
                writer.Append($"{_method}(");

            WriteField(writer, _fieldName);

            if (_method != GroupByMethod.None)
                writer.Append(")");
        }
    }
}
