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
            writer
                .Append("key(");

            if (_fieldName != null)
                writer.Append(_fieldName);

            writer.Append(")");

            if (_projectedName == null || _fieldName == _projectedName)
                return;

            writer
                .Append(" as ")
                .Append(_projectedName);
        }
    }
}