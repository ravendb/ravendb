using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ICSharpCode.NRefactory.CSharp.Refactoring;
using QUT.GplexBuffers;
using QUT.Gppg;

namespace Raven.Database.Indexing
{
    internal partial class LuceneQueryScanner
    {
        public bool InMethod;

        public string HandleTermInMethod()
        {
            var commaIndex = yytext.IndexOf(',');
            if (commaIndex == -1) return yytext;
            var firstTerm = yytext.Substring(0, commaIndex);
            var lastCommaIndex = yytext.LastIndexOf(',');
            var newSource = yytext.Substring(commaIndex, lastCommaIndex - commaIndex);//.Replace(",", " , ");
            var rewind = yytext.Length - lastCommaIndex - 1;
            tokTxt = null;
            tokECol -= rewind;
            tokEPos -= rewind;
            buffer.Pos -= (rewind + 1);
            code = 44;
            yylloc = new LexLocation(tokLin - 1, tokCol, tokELin - 1, tokECol);
            if (lastCommaIndex != commaIndex)
            {
                var currentContext = MkBuffCtx();
                byte[] inputBuffer = System.Text.Encoding.Default.GetBytes(newSource);
                MemoryStream stream = new MemoryStream(inputBuffer);
                SetSource(stream);
                (buffer as BuildBuffer).SetPaddingOn = true;
                bStack.Push(currentContext);
            }
            //CommaPaddingStream
            return firstTerm;
        }

        public void PublicSetSource(string source)
        {
            SetSource(source, 0);
        }

        protected override bool yywrap()
        {
            if (bStack.Count == 0) return true;
            RestoreBuffCtx(bStack.Pop()); return false;
        }
        Stack<BufferContext> bStack = new Stack<BufferContext>();
		public override void yyerror(string format, params object[] args)
		{
			base.yyerror(format, args);
			Console.WriteLine(format, args);
			Console.WriteLine();
		}
    }
}
