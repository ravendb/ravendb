using System;
using System.Text;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    public class RevisionIncludesToken : QueryToken
    {
        private string _sourcePath;
        private readonly string _dateTime;
        private readonly string _changeVector;
        
        
        private RevisionIncludesToken(DateTime dateTime)
        {
            _dateTime = dateTime.GetDefaultRavenFormat();
        }
        private RevisionIncludesToken(string sourcePath, string changeVector)
        {
            _sourcePath = sourcePath;
            _changeVector = changeVector;
        }
        
        internal static RevisionIncludesToken Create(DateTime dateTime)
        {
            return new RevisionIncludesToken(dateTime);
        }
        internal static RevisionIncludesToken Create(string sourcePath, string changeVector)
        {
            return new RevisionIncludesToken(sourcePath, changeVector);
        }
        public void AddAliasToPath(string alias)
        {
            _sourcePath ??= alias;
        }
        
        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("revisions(");
            if (_dateTime != null)
            {
                writer.Append('\'');
                writer.Append(_dateTime);
                writer.Append('\'');

            }

            else if(string.IsNullOrEmpty(_changeVector) == false)
            {
                writer.Append($"{_sourcePath}.{_changeVector}");
            }
            writer.Append(')');
        }
    }
}

