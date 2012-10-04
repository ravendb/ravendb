using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class CaseClause {
        public Expression Expression { get; set; }
        public BlockStatement Statements { get; private set; }

        public CaseClause() {
            Statements = new BlockStatement();
        }
    }
}
