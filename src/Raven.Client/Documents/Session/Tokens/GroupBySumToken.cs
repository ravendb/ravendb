using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class GroupBySumToken : QueryToken
    {
        private readonly string _projectedName;
        private readonly string _fieldName;

        private GroupBySumToken(string fieldName, string projectedName)
        {
            _fieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
            _projectedName = projectedName;
        }

        public static GroupBySumToken Create(string fieldName, string projectedName)
        {
            return new GroupBySumToken(fieldName, projectedName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("sum(")
                .Append(_fieldName)
                .Append(")");

            if (_projectedName == null)
                return;

            writer
                .Append(" as ")
                .Append(_projectedName);
        }
    }
}