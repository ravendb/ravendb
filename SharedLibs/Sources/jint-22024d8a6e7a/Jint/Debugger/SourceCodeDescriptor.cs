using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Debugger {
    [Serializable]
    public class SourceCodeDescriptor {
        [Serializable]
        public class Location {
            public Location(int line, int c) {
                this.line = line;
                this._Char = c;
            }

            private int line;

            public int Line {
                get { return line; }
                set { line = value; }
            }
            private int _Char;

            public int Char {
                get { return _Char; }
                set { _Char = value; }
            }
        }

        protected Location start;

        public Location Start {
            get { return start; }
            set { start = value; }
        }
        protected Location stop;

        public Location Stop {
            get { return stop; }
            set { stop = value; }
        }

        public string Code { get; private set; }

        public SourceCodeDescriptor(int startLine, int startChar, int stopLine, int stopChar, string code) {
            Code = code;

            Start = new Location(startLine, startChar);
            Stop = new Location(stopLine, stopChar);

        }

        public override string ToString() {
            return "Line: " + Start.Line + " Char: " + Start.Char;
        }
    }
}
