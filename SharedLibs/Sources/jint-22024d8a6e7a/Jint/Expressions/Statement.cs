using System;
using Antlr.Runtime.Tree;
using Jint.Debugger;

namespace Jint.Expressions {
    [Serializable]
    public abstract class Statement {
        public string Label { get; set; }

        public abstract void Accept(IStatementVisitor visitor);
        protected SourceCodeDescriptor source;

        public SourceCodeDescriptor Source {
            get { return source; }
            set { source = value; }
        }

        public Statement() {
            Label = String.Empty;
        }

		public override string ToString()
		{
			var jsCodeVisitor = new JsCodeVisitor();
			Accept(jsCodeVisitor);
			return jsCodeVisitor.Builder.ToString();
		}
    }
}
