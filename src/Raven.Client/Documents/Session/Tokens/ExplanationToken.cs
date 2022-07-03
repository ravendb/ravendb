using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class ExplanationToken : QueryToken
    {
        
        private string _optionsParameterName;
        private bool _optionsParameterNameSet = false;
        public string OptionsParameterName
        {
            get => _optionsParameterName;
            set
            {
                if (_optionsParameterNameSet == false)
                {
                    _optionsParameterNameSet = true;
                    this._optionsParameterName = value; // _optionsParameterName can set only once
                }
                else
                {
                    throw new InvalidOperationException("OptionsParameterName can be set only once");
                }
            }
        }

        private ExplanationToken()
        {
        }

        private ExplanationToken(string optionsParameterName)
        {
            OptionsParameterName = optionsParameterName;
        }

        public static ExplanationToken Create(string optionsParameterName = null)
        {
            if (optionsParameterName == null)
            {
                return new ExplanationToken();
            }
            return new ExplanationToken(optionsParameterName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("explanations(");

            if (OptionsParameterName != null)
            {
                writer
                    .Append("$")
                    .Append(OptionsParameterName);
            }

            writer
                .Append(")");
        }
    }
}
