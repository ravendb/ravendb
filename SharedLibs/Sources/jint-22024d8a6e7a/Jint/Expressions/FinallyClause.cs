using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class FinallyClause {
        public Statement Statement { get; set; }

        public FinallyClause(Statement statement) {
            Statement = statement;
        }
    }
}
