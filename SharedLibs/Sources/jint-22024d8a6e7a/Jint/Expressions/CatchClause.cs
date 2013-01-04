using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class CatchClause {
        public string Identifier { get; set; }
        public Statement Statement { get; set; }

        public CatchClause(string identifier, Statement statement) {
            Identifier = identifier;
            Statement = statement;
        }
    }
}
