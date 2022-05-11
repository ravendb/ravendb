using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class GroupByKeyToken : QueryToken
    {
        private readonly string _fieldName;
        private readonly string _projectedName;

        private GroupByKeyToken(string fieldName, string projectedName)
        {
            _fieldName = fieldName;
            _projectedName = projectedName;
        }

        public static GroupByKeyToken Create(string fieldName, string projectedName)
        {
            return new GroupByKeyToken(fieldName, projectedName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            WriteField(writer, _fieldName ?? "key()");

            if (_projectedName == null || _projectedName == _fieldName)
                return;

            writer
                .Append(" as '")
                .Append(_projectedName)
                .Append("'");
        }
    }
}
