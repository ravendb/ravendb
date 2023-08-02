using System;
using System.Text;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class RevisionIncludesToken : QueryToken
    {
        private readonly string _dateTime;
        private readonly string _path;
        
        private RevisionIncludesToken(DateTime dateTime)
        {
            _dateTime = dateTime.GetDefaultRavenFormat();
        }
        private RevisionIncludesToken(string path)
        {
            _path = path;
        }
        
        internal static RevisionIncludesToken Create(DateTime dateTime)
        {
            return new RevisionIncludesToken(dateTime);
        }
        internal static RevisionIncludesToken Create(string path)
        {
            return new RevisionIncludesToken(path);
        }
        
        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("revisions(\'");
            if (_dateTime != null)
            {
                writer.Append(_dateTime);

            }
            else if(string.IsNullOrEmpty(_path) == false)
            {
                writer.Append(_path);
            }
            writer.Append("\')");
        }
    }
}

