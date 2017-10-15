using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class LoadToken : QueryToken
    {
        public string Argument { get; }

        public string Alias { get; set; }

        private LoadToken(string argument, string alias)
        {
            Argument = argument;
            Alias = alias;
        }

        public static LoadToken Create(string argument, string alias)
        {
            return new LoadToken(argument , alias);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append(Argument)
                .Append(" as ")
                .Append(Alias);
        }
    }
}
