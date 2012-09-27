using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class RegexpExpression : Expression {
        public string Regexp { get; set; }
        public string Options { get; set; }

        public RegexpExpression(string regexp) {
            Regexp = regexp;
        }

        public RegexpExpression(string regexp, string options)
            : this(regexp) {
            Options = options;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
