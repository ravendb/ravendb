/*

Copyrights 2008-2009 Xebic Reasearch BV. All rights reserved (see license.txt).
Original work by Patrick Hulsmeijer.

This ANTLR 3 LL(*) grammar is based on Ecma-262 3rd edition (JavaScript 1.5, JScript 5.5). 
The annotations refer to the "A Grammar Summary" section (e.g. A.1 Lexical Grammar) and the numbers in parenthesis to the paragraph numbers (e.g. (7.8) ).
This document is best viewed with ANTLRWorks (www.antlr.org).


The major challenges faced in defining this grammar were:

-1- Ambiguity surrounding the DIV sign in relation to the multiplicative expression and the regular expression literal.
This is solved with some lexer driven magic: a gated semantical predicate turns the recognition of regular expressions on or off, based on the
value of the RegularExpressionsEnabled property. When regular expressions are enabled they take precedence over division expressions. The decision whether
regular expressions are enabled is based on the heuristics that the previous token can be considered as last token of a left-hand-side operand of a division.

-2- Automatic semicolon insertion.
This is solved within the parser. The semicolons are not physically inserted but the situations in which they should are recognized and treated as if they were.
The physical insertion of semicolons would be undesirable because of several reasons:
- performance degration because of how ANTLR handles tokens in token streams
- the alteration of the input, which we need to have unchanged
- it is superfluous being of no interest to AST construction

-3- Unicode identifiers
Because ANTLR couldn't handle the unicode tables defined in the specification well and for performance reasons unicode identifiers are implemented as an action 
driven alternative to ASCII identifiers. First the ASCII version is tried that is defined in detail in this grammar and then the unicode alternative is tried action driven.
Because of the fact that the ASCII version is defined in detail the mTokens switch generation in the lexer can predict identifiers appropriately.
For details see the identifier rules.


The minor challenges were related to converting the grammar to an ANTLR LL(*) grammar:
- Resolving the ambiguity between functionDeclaration vs functionExpression and block vs objectLiteral stemming from the expressionStatement production.
- Left recursive nature of the left hand side expressions.
- The assignmentExpression production.
- The forStatement production.
The grammar was kept as close as possible to the grammar in the "A Grammar Summary" section of Ecma-262.

*/

grammar ES3 ;

options
{
	output = AST ;
	language = CSharp3 ;
}


tokens
{
// Reserved words
	NULL		= 'null' ;
	TRUE		= 'true' ;
	FALSE		= 'false' ;

// Keywords
	BREAK		= 'break' ;
	CASE		= 'case' ;
	CATCH 		= 'catch' ;
	CONTINUE 	= 'continue' ;
	DEFAULT		= 'default' ;
	DELETE		= 'delete' ;
	DO 		= 'do' ;
	ELSE 		= 'else' ;
	FINALLY 	= 'finally' ;
	FOR 		= 'for' ;
	FUNCTION 	= 'function' ;
	IF 		= 'if' ;
	IN 		= 'in' ;
	INSTANCEOF 	= 'instanceof' ;
	NEW 		= 'new' ;
	RETURN 		= 'return' ;
	SWITCH 		= 'switch' ;
	THIS 		= 'this' ;
	THROW 		= 'throw' ;
	TRY 		= 'try' ;
	TYPEOF 		= 'typeof' ;
	VAR 		= 'var' ;
	VOID 		= 'void' ;
	WHILE 		= 'while' ;
	WITH 		= 'with' ;

// Future reserved words
	ABSTRACT	= 'abstract' ;
	BOOLEAN 	= 'boolean' ;
	BYTE 		= 'byte' ;
	CHAR 		= 'char' ;
	CLASS 		= 'class' ;
	CONST 		= 'const' ;
	DEBUGGER 	= 'debugger' ;
	DOUBLE		= 'double' ;
	ENUM 		= 'enum' ;
	EXPORT 		= 'export' ;
	EXTENDS		= 'extends' ;
	FINAL 		= 'final' ;
	FLOAT 		= 'float' ;
	GOTO 		= 'goto' ;
	IMPLEMENTS 	= 'implements' ;
	IMPORT		= 'import' ;
	INT 		= 'int' ;
	INTERFACE 	= 'interface' ;
	LONG 		= 'long' ;
	NATIVE 		= 'native' ;
	PACKAGE 	= 'package' ;
	PRIVATE 	= 'private' ;
	PROTECTED 	= 'protected' ;
	PUBLIC		= 'public' ;
	SHORT 		= 'short' ;
	STATIC 		= 'static' ;
	SUPER 		= 'super' ;
	SYNCHRONIZED 	= 'synchronized' ;
	THROWS 		= 'throws' ;
	TRANSIENT 	= 'transient' ;
	VOLATILE 	= 'volatile' ;

// Punctuators
	LBRACE		= '{' ;
	RBRACE		= '}' ;
	LPAREN		= '(' ;
	RPAREN		= ')' ;
	LBRACK		= '[' ;
	RBRACK		= ']' ;
	DOT		= '.' ;
	SEMIC		= ';' ;
	COMMA		= ',' ;
	LT		= '<' ;
	GT		= '>' ;
	LTE		= '<=' ;
	GTE		= '>=' ;
	EQ		= '==' ;
	NEQ		= '!=' ;
	SAME		= '===' ;
	NSAME		= '!==' ;
	ADD		= '+' ;
	SUB		= '-' ;
	MUL		= '*' ;
	MOD		= '%' ;
	INC		= '++' ;
	DEC		= '--' ;
	SHL		= '<<' ;
	SHR		= '>>' ;
	SHU		= '>>>' ;
	AND		= '&' ;
	OR		= '|' ;
	XOR		= '^' ;
	NOT		= '!' ;
	INV		= '~' ;
	LAND		= '&&' ;
	LOR		= '||' ;
	QUE		= '?' ;
	COLON		= ':' ;
	ASSIGN		= '=' ;
	ADDASS		= '+=' ;
	SUBASS		= '-=' ;
	MULASS		= '*=' ;
	MODASS		= '%=' ;
	SHLASS		= '<<=' ;
	SHRASS		= '>>=' ;
	SHUASS		= '>>>=' ;
	ANDASS		= '&=' ;
	ORASS		= '|=' ;
	XORASS		= '^=' ;
	DIV		= '/' ;
	DIVASS		= '/=' ;
	
// Imaginary
	ARGS ;
	ARRAY ;
	BLOCK ;
	BYFIELD ;
	BYINDEX ;
	CALL ;
	CEXPR ;
	EXPR ;
	FORITER ;
	FORSTEP ;
	ITEM ;
	LABELLED ;
	NAMEDVALUE ;
	NEG ;
	OBJECT ;
	PAREXPR ;
	PDEC ;
	PINC ;
	POS ;

}

@lexer::members
{
    private IToken last;

    private bool AreRegularExpressionsEnabled()
    {
    	if (last == null)
    	{
    		return true;
    	}
    	switch (last.Type)
    	{
    	// identifier
    		case Identifier:
    	// literals
    		case NULL:
    		case TRUE:
    		case FALSE:
    		case THIS:
    		case OctalIntegerLiteral:
    		case DecimalLiteral:
    		case HexIntegerLiteral:
    		case StringLiteral:
    	// member access ending 
    		case RBRACK:
    	// function call or nested expression ending
    		case RPAREN:
    			return false;
    	// otherwise OK
    		default:
    			return true;
    	}
    }
    	
    private void ConsumeIdentifierUnicodeStart()
    {
    	int ch = input.LA(1);
    	if (IsIdentifierStartUnicode(ch))
    	{
    		MatchAny();
    		do
    		{
    			ch = input.LA(1);
    			if (ch == '$' || (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || ch == '\\' || ch == '_' || (ch >= 'a' && ch <= 'z') || IsIdentifierPartUnicode(ch))
    			{
    				mIdentifierPart();
    			}
    			else
    			{
    				return;
    			}
    		}
    		while (true);
    	}
    	else
    	{
    		throw new NoViableAltException();
    	}
    }

    private bool IsIdentifierPartUnicode(int ch)
    {
        return char.IsLetterOrDigit((char)ch);
    }

    private bool IsIdentifierStartUnicode(int ch)
    {
        return char.IsLetter((char)ch);
    }

    public override IToken NextToken()
    {
    	IToken result = base.NextToken();
    	if (result.Channel == DefaultTokenChannel)
    	{
    		last = result;
    	}
    	return result;		
    }
   
}

@header {
using System;
using System.Text;
using System.Globalization;
using System.Collections.Generic;
using Jint.Expressions;
using Jint.Debugger;
}

@parser::members
{
		// References the upper level block currently parsed. 
		// This is used to add variable declarations at the top of the body while parsing.
		private LinkedList<Statement> _currentBody = null;
		
		// Set to true when a New is in parenthesis, to prevent the MemberExpression
		// from appending new members to it
		private bool _newExpressionIsUnary = false;
		
		private const char BS = '\\';
		private bool IsLeftHandSideAssign(Expression lhs, object[] cached)
		{
    		if (cached[0] != null)
    		{
    			return System.Convert.ToBoolean(cached[0]);
    		}
	    	
    		bool result;
    		if(IsLeftHandSideExpression(lhs))
    		{
    			switch (input.LA(1))
    			{
    				case ASSIGN:
    				case MULASS:
    				case DIVASS:
    				case MODASS:
    				case ADDASS:
    				case SUBASS:
    				case SHLASS:
    				case SHRASS:
    				case SHUASS:
    				case ANDASS:
    				case XORASS:
    				case ORASS:
    					result = true;
    					break;
    				default:
    					result = false;
    					break;
    			}
    		}
    		else
    		{
    			result = false;
    		}
	    	
    		cached[0] = result;
    		return result;
		}

		private static bool IsLeftHandSideExpression(Expression lhs)
		{
			if (lhs == null)
			{
				return true;
			}

			return lhs is Identifier || lhs is PropertyExpression || lhs is MemberExpression;
		}
	    	
		private bool IsLeftHandSideIn(Expression lhs, object[] cached)
		{
    		if (cached[0] != null)
    		{
    			return System.Convert.ToBoolean(cached[0]);
    		}
	    	
    		bool result = IsLeftHandSideExpression(lhs) && (input.LA(1) == IN);
    		cached[0] = result;
    		return result;
		}

		private void PromoteEOL(ParserRuleReturnScope<IToken> rule)
		{
    		// Get current token and its type (the possibly offending token).
    		IToken lt = input.LT(1);
    		int la = lt.Type;
	    	
    		// We only need to promote an EOL when the current token is offending (not a SEMIC, EOF, RBRACE, EOL or MultiLineComment).
    		// EOL and MultiLineComment are not offending as they're already promoted in a previous call to this method.
    		// Promoting an EOL means switching it from off channel to on channel.
    		// A MultiLineComment gets promoted when it contains an EOL.
    		if (!(la == SEMIC || la == EOF || la == RBRACE || la == EOL || la == MultiLineComment))
    		{
    			// Start on the possition before the current token and scan backwards off channel tokens until the previous on channel token.
    			for (int ix = lt.TokenIndex - 1; ix > 0; ix--)
    			{
    				lt = input.Get(ix);
    				if (lt.Channel == DefaultTokenChannel)
    				{
    					// On channel token found: stop scanning.
    					break;
    				}
    				else if (lt.Type == EOL || (lt.Type == MultiLineComment && (lt.Text.EndsWith("\r") || lt.Text.EndsWith("\n"))))
    				{
    					// We found our EOL: promote the token to on channel, position the input on it and reset the rule start.
    					lt.Channel = DefaultTokenChannel;
    					input.Seek(lt.TokenIndex);
    					if (rule != null)
    					{
    						rule.Start = lt;
    					}
    					break;
    				}
    			}
    		}
		}	
	    
		private static NumberFormatInfo numberFormatInfo = new NumberFormatInfo();

		private string extractRegExpPattern(string text) {
			return text.Substring(1, text.LastIndexOf('/')-1);
		}

		private string extractRegExpOption(string text) {
			if(text[text.Length-1] != '/')
			{
			return text.Substring(text.LastIndexOf('/')+1);
			}
			return String.Empty;
		}
    
		private static Encoding Latin1 = Encoding.GetEncoding("iso-8859-1");
    
	    private string extractString(string text) {
	    
	    // https://developer.mozilla.org/en/Core_JavaScript_1.5_Guide/Literals#String Literals    
	        StringBuilder sb = new StringBuilder(text.Length);
	        int startIndex = 1; // Skip initial quote
	        int slashIndex = -1;

	        while ((slashIndex = text.IndexOf(BS, startIndex)) != -1)
	        {
                sb.Append(text.Substring(startIndex, slashIndex - startIndex));
	            char escapeType = text[slashIndex + 1];
	            switch (escapeType)
	            {
	                case '0':
	                case '1':
	                case '2':
	                case '3':
	                case '4':
	                case '5':
	                case '6':
	                case '7':
	                case '8':
	                case '9':
                        string octalCode = text.Substring(slashIndex + 1, 3);   
                        char octalChar = Latin1.GetChars(new byte[] { System.Convert.ToByte(octalCode, 8) } )[0]; 
                        // insert decoded char
                        sb.Append(octalChar);
                        // skip encoded char
                        slashIndex += 4;
			          break;                 
	                case 'x':
                        string asciiCode = text.Substring(slashIndex + 2, 2); ;
                        char asciiChar = Latin1.GetChars(new byte[] { System.Convert.ToByte(asciiCode, 16) } )[0];
                        sb.Append(asciiChar);
                        slashIndex += 4;
                        break;   	
	                case 'u':
                        char unicodeChar = System.Convert.ToChar(Int32.Parse(text.Substring(slashIndex + 2, 4), System.Globalization.NumberStyles.AllowHexSpecifier));
                        sb.Append(unicodeChar);
                        slashIndex += 6;
                        break;
                    case 'b': sb.Append('\b'); slashIndex += 2; break;
                    case 'f': sb.Append('\f'); slashIndex += 2; break;
                    case 'n': sb.Append('\n'); slashIndex += 2; break;
                    case 'r': sb.Append('\r'); slashIndex += 2; break;
                    case 't': sb.Append('\t'); slashIndex += 2; break;
                    case 'v': sb.Append('\v'); slashIndex += 2; break;
                    case '\'': sb.Append('\''); slashIndex += 2; break;
                    case '"': sb.Append('"'); slashIndex += 2; break;
                    case '\\': sb.Append('\\'); slashIndex += 2; break;
                    case '\r': if (text[slashIndex + 2] == '\n') slashIndex += 3; break;
                    case '\n': slashIndex += 2; break;
                    default: sb.Append(escapeType); slashIndex += 2; break;
	            }

                startIndex = slashIndex;
	        }

            if (sb.Length == 0)
                return text.Substring(1, text.Length - 2);

            sb.Append(text.Substring(startIndex, text.Length - startIndex - 1));
	        return sb.ToString();
	    }
	    
		public List<string> Errors { get; private set; }

		public override void DisplayRecognitionError(String[] tokenNames, RecognitionException e) {
	        
			base.DisplayRecognitionError(tokenNames, e);
	        
			if(Errors == null)
			{
        		Errors = new List<string>();
			}
	        
			String hdr = GetErrorHeader(e);
			String msg = GetErrorMessage(e, tokenNames);
			Errors.Add(msg + " at " + hdr);
		}    

		private string[] script = new string[0];
	    
    		public bool DebugMode { get; set; }
	    	
			private SourceCodeDescriptor ExtractSourceCode(CommonToken start, CommonToken stop)
			{
				if(!DebugMode)
				{
            		return new SourceCodeDescriptor(start.Line, start.CharPositionInLine, stop.Line, stop.CharPositionInLine, "No source code available.");
				}
	            
				try
				{
					StringBuilder source = new StringBuilder();

					for (int i = start.Line - 1; i <= stop.Line - 1; i++)
					{
						int charStart = 0;
						int charStop = script[i].Length;

						if (i == start.Line - 1)
						{
							charStart = start.CharPositionInLine;
						}

						if (i == stop.Line - 1)
						{
							charStop = stop.CharPositionInLine;
						}

						int length = charStop - charStart;

						source.Append(script[i].Substring(charStart, length)).Append(Environment.NewLine);
					}

					return new SourceCodeDescriptor(start.Line, start.CharPositionInLine, stop.Line, stop.CharPositionInLine, source.ToString());
				}
				catch
				{
					return new SourceCodeDescriptor(start.Line, start.CharPositionInLine, stop.Line, stop.CharPositionInLine, "No source code available.");
				}

			}

		public AssignmentOperator ResolveAssignmentOperator(string op)
		{
    		switch(op)
    		{
    			case "=" : return AssignmentOperator.Assign;
    			case "+=" : return AssignmentOperator.Add;
    			case "-=" : return AssignmentOperator.Substract;
    			case "*=" : return AssignmentOperator.Multiply;
    			case "\%=" : return AssignmentOperator.Modulo;
    			case "<<=" : return AssignmentOperator.ShiftLeft;
    			case ">>=" : return AssignmentOperator.ShiftRight;
    			case ">>>=" : return AssignmentOperator.UnsignedRightShift;
    			case "&=" : return AssignmentOperator.And;
    			case "|=" : return AssignmentOperator.Or;
    			case "^=" : return AssignmentOperator.XOr;
    			case "/=" : return AssignmentOperator.Divide;
    			default : throw new NotSupportedException("Invalid assignment operator: " + op);
    		}
		}
}

@init {
    numberFormatInfo.NumberDecimalSeparator = ".";
}

//
// $<	A.1 Lexical Grammar (7)
//

// Added for lexing purposes

fragment BSLASH
	: '\\'
	;
	
fragment DQUOTE
	: '"'
	;
	
fragment SQUOTE
	: '\''
	;

// $<	Whitespace (7.2)

fragment TAB
	: '\u0009'
	;

fragment VT // Vertical TAB
	: '\u000b'
	;

fragment FF // Form Feed
	: '\u000c'
	;

fragment SP // Space
	: '\u0020'
	;

fragment NBSP // Non-Breaking Space
	: '\u00a0'
	;

fragment USP // Unicode Space Separator (rest of Unicode category Zs)
	: '\u1680'  // OGHAM SPACE MARK
	| '\u180E'  // MONGOLIAN VOWEL SEPARATOR
	| '\u2000'  // EN QUAD
	| '\u2001'  // EM QUAD
	| '\u2002'  // EN SPACE
	| '\u2003'  // EM SPACE
	| '\u2004'  // THREE-PER-EM SPACE
	| '\u2005'  // FOUR-PER-EM SPACE
	| '\u2006'  // SIX-PER-EM SPACE
	| '\u2007'  // FIGURE SPACE
	| '\u2008'  // PUNCTUATION SPACE
	| '\u2009'  // THIN SPACE
	| '\u200A'  // HAIR SPACE
	| '\u202F'  // NARROW NO-BREAK SPACE
	| '\u205F'  // MEDIUM MATHEMATICAL SPACE
	| '\u3000'  // IDEOGRAPHIC SPACE
	;

WhiteSpace
	: ( TAB | VT | FF | SP | NBSP | USP )+ { $channel = Hidden; }
	;

// $>

// $<	Line terminators (7.3)

fragment LF // Line Feed
	: '\n'
	;

fragment CR // Carriage Return
	: '\r'
	;

fragment LS // Line Separator
	: '\u2028'
	;

fragment PS // Paragraph Separator
	: '\u2029'
	;

fragment LineTerminator
	: CR | LF | LS | PS
	;
		
EOL
	: ( ( CR LF ) | LF | LS | PS ) { $channel = Hidden; }
	;
// $>

// $<	Comments (7.4)

MultiLineComment
	: '/*' ( options { greedy = false; } : . )* '*/' { $channel = Hidden; }
	;

SingleLineComment
	: '//' ( ~( LineTerminator ) )* { $channel = Hidden; }
	;

// $>

// $<	Tokens (7.5)

token
	: reservedWord
	| Identifier
	| punctuator
	| numericLiteral
	| StringLiteral
	;

// $<	Reserved words (7.5.1)

reservedWord
	: keyword
	| futureReservedWord
	| NULL
	| booleanLiteral
	;

// $>
	
// $<	Keywords (7.5.2)

keyword
	: BREAK
	| CASE
	| CATCH
	| CONTINUE
	| DEFAULT
	| DELETE
	| DO
	| ELSE
	| FINALLY
	| FOR
	| FUNCTION
	| IF
	| IN
	| INSTANCEOF
	| NEW
	| RETURN
	| SWITCH
	| THIS
	| THROW
	| TRY
	| TYPEOF
	| VAR
	| VOID
	| WHILE
	| WITH
	;

// $>

// $<	Future reserved words (7.5.3)

futureReservedWord
	: ABSTRACT
	| BOOLEAN
	| BYTE
	| CHAR
	| CLASS
	| CONST
	| DEBUGGER
	| DOUBLE
	| ENUM
	| EXPORT
	| EXTENDS
	| FINAL
	| FLOAT
	| GOTO
	| IMPLEMENTS
	| IMPORT
	| INT
	| INTERFACE
	| LONG
	| NATIVE
	| PACKAGE
	| PRIVATE
	| PROTECTED
	| PUBLIC
	| SHORT
	| STATIC
	| SUPER
	| SYNCHRONIZED
	| THROWS
	| TRANSIENT
	| VOLATILE
	;

// $>

// $>
	
// $<	Identifiers (7.6)

fragment IdentifierStartASCII
	: 'a'..'z' | 'A'..'Z'
	| '$'
	| '_'
	| BSLASH 'u' HexDigit HexDigit HexDigit HexDigit // UnicodeEscapeSequence
	;

/*
The first two alternatives define how ANTLR can match ASCII characters which can be considered as part of an identifier.
The last alternative matches other characters in the unicode range that can be sonsidered as part of an identifier.
*/
fragment IdentifierPart
	: DecimalDigit
	| IdentifierStartASCII
	| { IsIdentifierPartUnicode(input.LA(1)) }? { MatchAny(); }
	;

fragment IdentifierNameASCIIStart
	: IdentifierStartASCII IdentifierPart*
	;

/*
The second alternative acts as an action driven fallback to evaluate other characters in the unicode range than the ones in the ASCII subset.
Due to the first alternative this grammar defines enough so that ANTLR can generate a lexer that correctly predicts identifiers with characters in the ASCII range.
In that way keywords, other reserved words and ASCII identifiers are recognized with standard ANTLR driven logic. When the first character for an identifier fails to 
match this ASCII definition, the lexer calls ConsumeIdentifierUnicodeStart because of the action in the alternative. This method checks whether the character matches 
as first character in ranges other than ASCII and consumes further characters belonging to the identifier with help of mIdentifierPart generated out of the 
IdentifierPart rule above.
*/
Identifier
	: IdentifierNameASCIIStart
	| { ConsumeIdentifierUnicodeStart(); }
	;

// $>

// $<	Punctuators (7.7)

punctuator
	: LBRACE
	| RBRACE
	| LPAREN
	| RPAREN
	| LBRACK
	| RBRACK
	| DOT
	| SEMIC
	| COMMA
	| LT
	| GT
	| LTE
	| GTE
	| EQ
	| NEQ
	| SAME
	| NSAME
	| ADD
	| SUB
	| MUL
	| MOD
	| INC
	| DEC
	| SHL
	| SHR
	| SHU
	| AND
	| OR
	| XOR
	| NOT
	| INV
	| LAND
	| LOR
	| QUE
	| COLON
	| ASSIGN
	| ADDASS
	| SUBASS
	| MULASS
	| MODASS
	| SHLASS
	| SHRASS
	| SHUASS
	| ANDASS
	| ORASS
	| XORASS
	| DIV
	| DIVASS
	;

// $>

// $<	Literals (7.8)

literal returns [Expression value]
	: exp1=NULL { $value = new Identifier(exp1.Text); }
	| exp2=booleanLiteral { $value = new ValueExpression(exp2.value, TypeCode.Boolean); }
	| exp3=numericLiteral { $value = new ValueExpression(exp3.value, TypeCode.Double); }
	| exp4=StringLiteral  { $value = new ValueExpression(extractString(exp4.Text), TypeCode.String); }
	| exp5=RegularExpressionLiteral { $value = new RegexpExpression(extractRegExpPattern(exp5.Text), extractRegExpOption(exp5.Text)); }
	;

booleanLiteral returns [bool value]
	: TRUE { $value = true; }
	| FALSE { $value = false; }
	;

// $<	Numeric literals (7.8.3)

/*
Note: octal literals are described in the B Compatibility section.
These are removed from the standards but are here for backwards compatibility with earlier ECMAScript definitions.
*/

fragment DecimalDigit
	: '0'..'9'
	;

fragment HexDigit
	: DecimalDigit | 'a'..'f' | 'A'..'F'
	;

fragment OctalDigit
	: '0'..'7'
	;

fragment ExponentPart
	: ( 'e' | 'E' ) ( '+' | '-' )? DecimalDigit+
	;

fragment DecimalIntegerLiteral
	: '0'
	| '1'..'9' DecimalDigit*
	;

DecimalLiteral
	: DecimalIntegerLiteral '.' DecimalDigit* ExponentPart?
	| '.' DecimalDigit+ ExponentPart?
	| DecimalIntegerLiteral ExponentPart?
	;

OctalIntegerLiteral
	: '0' OctalDigit+
	;

HexIntegerLiteral
	: ( '0x' | '0X' ) HexDigit+
	;

numericLiteral returns [double value]
	: ex1=DecimalLiteral { $value = double.Parse(ex1.Text, NumberStyles.Float, numberFormatInfo); }
	| ex2=OctalIntegerLiteral { $value = System.Convert.ToInt64(ex2.Text, 8); }
	| ex3=HexIntegerLiteral { $value = System.Convert.ToInt64(ex3.Text, 16); }
	;

// $>

// $<	String literals (7.8.4)

/*
Note: octal escape sequences are described in the B Compatibility section.
These are removed from the standards but are here for backwards compatibility with earlier ECMAScript definitions.
*/
	
fragment CharacterEscapeSequence
	: ~( DecimalDigit | 'x' | 'u' | LineTerminator ) // Concatenation of SingleEscapeCharacter and NonEscapeCharacter
	;

fragment ZeroToThree
	: '0'..'3'
	;
	
fragment OctalEscapeSequence
	: OctalDigit
	| ZeroToThree OctalDigit
	| '4'..'7' OctalDigit
	| ZeroToThree OctalDigit OctalDigit
	;
	
fragment HexEscapeSequence
	: 'x' HexDigit HexDigit
	;
	
fragment UnicodeEscapeSequence
	: 'u' HexDigit HexDigit HexDigit HexDigit
	;

fragment EscapeSequence
	:
	BSLASH 
	(
		CharacterEscapeSequence 
		| OctalEscapeSequence
		| HexEscapeSequence
		| UnicodeEscapeSequence
		| CR? LF // allow string continuations over a new line
	)
	;

StringLiteral
	: SQUOTE ( ~( SQUOTE | BSLASH | LineTerminator ) | EscapeSequence )* SQUOTE
	| DQUOTE ( ~( DQUOTE | BSLASH | LineTerminator ) | EscapeSequence )* DQUOTE
	;

// $>

// $<	Regular expression literals (7.8.5)

fragment BackslashSequence
	: BSLASH ~( LineTerminator )
	;

fragment RegularExpressionFirstChar
	: ~ ( LineTerminator | MUL | BSLASH | DIV )
	| BackslashSequence
	;

fragment RegularExpressionChar
	: ~ ( LineTerminator | BSLASH | DIV )
	| BackslashSequence
	;

RegularExpressionLiteral
	: { AreRegularExpressionsEnabled() }?=> DIV RegularExpressionFirstChar RegularExpressionChar* DIV IdentifierPart*
	;

// $>

// $>

// $>

//
// $<	A.3 Expressions (11)
//

// $<Primary expressions (11.1)

primaryExpression returns [Expression value]
	: ex1=THIS { $value = new Identifier(ex1.Text); }
	| ex2=Identifier { $value = new Identifier(ex2.Text); }
	| ex3=literal { $value = ex3.value; }
	| ex4=arrayLiteral { $value = ex4.value; }
	| ex5=objectLiteral { $value = ex5.value; }
	| lpar=LPAREN ex6=expression  RPAREN  { $value = ex6.value; _newExpressionIsUnary = ex6.value is NewExpression; } 
	;

arrayLiteral returns [ArrayDeclaration value]
@init {
	$value = new ArrayDeclaration();
}
	: lb=LBRACK ( first=arrayItem { if(first.value != null) $value.Parameters.Add(first.value); } ( COMMA follow=arrayItem  { if(follow.value != null) $value.Parameters.Add(follow.value); })* )? RBRACK
	
	;

arrayItem returns [Statement value]
	: ( expr=assignmentExpression  { $value = expr.value; } | { input.LA(1) == COMMA }? { $value = new Identifier("undefined"); } | { input.LA(1) == RBRACK }? { $value = null; }  )
	
	;

objectLiteral returns [JsonExpression value]
@init{
	$value = new JsonExpression();
}
	: lb=LBRACE ( first=propertyAssignment { $value.Push(first.value); }  ( COMMA follow=propertyAssignment { $value.Push(follow.value); } )* )? RBRACE
	;
	
propertyAssignment returns [PropertyDeclarationExpression value]
@init {
	$value = new PropertyDeclarationExpression();
	FunctionExpression func=new FunctionExpression();
}
	: acc=accessor { $value.Mode=acc.value; } { $value.Expression=func; } prop2=propertyName { $value.Name=func.Name=prop2.value; } (parameters=formalParameterList { func.Parameters.AddRange(parameters.value); })? statements=functionBody { func.Statement=statements.value; } 
	| prop1=propertyName { $value.Name=prop1.value; } COLON ass=assignmentExpression { $value.Expression=ass.value; }
	;
	
accessor returns [PropertyExpressionType value]
	: ex1=Identifier { ex1.Text=="get" || ex1.Text=="set" }?=> { if(ex1.Text=="get") $value= PropertyExpressionType.Get; if(ex1.Text=="set") $value=PropertyExpressionType.Set; }
	;

propertyName returns [string value]
	: ex1=Identifier { $value = ex1.Text; }
	| ex2=StringLiteral { $value = extractString(ex2.Text); }
	| ex3=numericLiteral { $value = ex3.value.ToString(); }
	;

// $>

// $<Left-hand-side expressions (11.2)

/*
Refactored some rules to make them LL(*) compliant:
all the expressions surrounding member selection and calls have been moved to leftHandSideExpression to make them right recursive
*/

memberExpression returns [Expression value]
	: prim=primaryExpression { $value = prim.value; }
	| func=functionExpression { $value = func.value; }
	| exp=newExpression { $value = exp.value; }
	;

newExpression returns [NewExpression value]
	: NEW^ first=memberExpression { $value = new NewExpression(first.value); }
	;
	
arguments returns [List<Expression> value]
@init {
	$value = new List<Expression>();
}
	: LPAREN ( first=assignmentExpression { $value.Add(first.value); } ( COMMA follow=assignmentExpression { $value.Add(follow.value); })* )? RPAREN
	
	;

generics returns [List<Expression> value]
@init {
	$value = new List<Expression>();
}
	: LBRACE ( first=assignmentExpression { $value.Add(first.value); } ( COMMA follow=assignmentExpression { $value.Add(follow.value); })* )? RBRACE
	
	;
	
	
leftHandSideExpression returns [Expression value]
@init {
	List<Expression> gens = new List<Expression>();
}
@after{
	$value.Source = ExtractSourceCode((CommonToken)retval.Start, (CommonToken)retval.Stop);
}
	:
	(
		mem=memberExpression { $value = mem.value; } 
	)
	(
		(gen=generics { gens = gen.value; } )? arg=arguments { if($value is NewExpression && !_newExpressionIsUnary) { ((NewExpression)$value).Generics = gens; ((NewExpression)$value).Arguments = arg.value; $value = new MemberExpression($value, null); } else { $value = new MemberExpression(new MethodCall(arg.value) { Generics = gens }, $value); } } 
	
		| LBRACK exp=expression RBRACK { $value = new MemberExpression(new Indexer(exp.value), $value); } 
			
		| DOT id=Identifier {  if($value is NewExpression && !_newExpressionIsUnary) { ((NewExpression)$value).Expression = new MemberExpression(new PropertyExpression(id.Text), ((NewExpression)$value).Expression); } else { $value = new MemberExpression(new PropertyExpression(id.Text), $value); } }
	)* 
	  
	;

// $>

// $<Postfix expressions (11.3)

/*
The specification states that there are no line terminators allowed before the postfix operators.
This is enforced by the call to PromoteEOL in the action before ( INC | DEC ).
We only must promote EOLs when the la is INC or DEC because this production is chained as all expression rules.
In other words: only promote EOL when we are really in a postfix expression. A check on the la will ensure this.
*/
postfixExpression returns [Expression value]
	: left=leftHandSideExpression { $value = left.value; if (input.LA(1) == INC || input.LA(1) == DEC) PromoteEOL(null);  } ( post=postfixOperator^ { $value = new UnaryExpression(post.value, $value); })?
	;
	
postfixOperator returns [UnaryExpressionType value]
	: op=INC { $op.Type = PINC; $value = UnaryExpressionType.PostfixPlusPlus; }
	| op=DEC { $op.Type = PDEC; $value = UnaryExpressionType.PostfixMinusMinus; }
	;

// $>

// $<Unary operators (11.4)

unaryExpression returns [Expression value]
	: post=postfixExpression { $value = post.value; }
	| op=unaryOperator^ exp=unaryExpression { $value = new UnaryExpression(op.value, exp.value); }
	;
	
unaryOperator returns [UnaryExpressionType value]
	: DELETE { $value = UnaryExpressionType.Delete; }
	| VOID { $value = UnaryExpressionType.Void; }
	| TYPEOF { $value = UnaryExpressionType.TypeOf; }
	| INC { $value = UnaryExpressionType.PrefixPlusPlus; }
	| DEC { $value = UnaryExpressionType.PrefixMinusMinus; }
	| op=ADD { $op.Type = POS; $value = UnaryExpressionType.Positive; }
	| op=SUB { $op.Type = NEG; $value = UnaryExpressionType.Negate; }
	| INV { $value = UnaryExpressionType.Inv; }
	| NOT { $value = UnaryExpressionType.Not; }
	;

// $>

// $<Multiplicative operators (11.5)

multiplicativeExpression returns [Expression value]
@init {
	BinaryExpressionType type = BinaryExpressionType.Unknown;
}
	: left=unaryExpression { $value = left.value; } ( 
		( MUL { type= BinaryExpressionType.Times; } 
		| DIV { type= BinaryExpressionType.Div; }
		| MOD { type= BinaryExpressionType.Modulo; })^ 
		right=unaryExpression { $value = new BinaryExpression(type, $value, right.value); })*
	;

// $>

// $<Additive operators (11.6)

additiveExpression returns [Expression value]
@init {
	BinaryExpressionType type = BinaryExpressionType.Unknown;
}
	: left=multiplicativeExpression { $value = left.value; } ( 
		( ADD { type= BinaryExpressionType.Plus; }
		| SUB { type= BinaryExpressionType.Minus; })^ 
		right=multiplicativeExpression { $value = new BinaryExpression(type, $value, right.value); })*
	;

// $>
	
// $<Bitwise shift operators (11.7)

shiftExpression returns [Expression value]
@init {
	BinaryExpressionType type = BinaryExpressionType.Unknown;
}
	: left=additiveExpression { $value = left.value; } ( 
		( SHL { type= BinaryExpressionType.LeftShift; }
		| SHR { type= BinaryExpressionType.RightShift; }
		| SHU { type= BinaryExpressionType.UnsignedRightShift; })^ 
		right=additiveExpression { $value = new BinaryExpression(type, $value, right.value); })*
	;

// $>
	
// $<Relational operators (11.8)

relationalExpression returns [Expression value]
@init {
	BinaryExpressionType type = BinaryExpressionType.Unknown;
}
	: left=shiftExpression { $value = left.value; } ( 
		( LT { type= BinaryExpressionType.Lesser; }
		| GT { type= BinaryExpressionType.Greater; }
		| LTE { type= BinaryExpressionType.LesserOrEqual; }
		| GTE { type= BinaryExpressionType.GreaterOrEqual; }
		| INSTANCEOF { type= BinaryExpressionType.InstanceOf;  }
		| IN { type= BinaryExpressionType.In;  })^ 
		right=shiftExpression { $value = new BinaryExpression(type, $value, right.value); })*
	;

relationalExpressionNoIn returns [Expression value]
@init {
	BinaryExpressionType type = BinaryExpressionType.Unknown;
}
	: left=shiftExpression { $value = left.value; } ( 
		( LT { type= BinaryExpressionType.Lesser; }
		| GT { type= BinaryExpressionType.Greater; }
		| LTE { type= BinaryExpressionType.LesserOrEqual; }
		| GTE { type= BinaryExpressionType.GreaterOrEqual; }
		| INSTANCEOF { type= BinaryExpressionType.InstanceOf;  } )^ 
		right=shiftExpression { $value = new BinaryExpression(type, $value, right.value); })*
	;

// $>
	
// $<Equality operators (11.9)

equalityExpression returns [Expression value]
@init {
	BinaryExpressionType type = BinaryExpressionType.Unknown;
}
	: left=relationalExpression { $value = left.value; } ( 
		( EQ { type= BinaryExpressionType.Equal; }
		| NEQ { type= BinaryExpressionType.NotEqual; }
		| SAME { type= BinaryExpressionType.Same; }
		| NSAME { type= BinaryExpressionType.NotSame; })^ 
		right=relationalExpression { $value = new BinaryExpression(type, $value, right.value); })*
	;

equalityExpressionNoIn returns [Expression value]
@init {
	BinaryExpressionType type = BinaryExpressionType.Unknown;
}
	: left=relationalExpressionNoIn { $value = left.value; } ( 
		( EQ { type= BinaryExpressionType.Equal; }
		| NEQ { type= BinaryExpressionType.NotEqual; }
		| SAME { type= BinaryExpressionType.Same; }
		| NSAME { type= BinaryExpressionType.NotSame; })^ 
		right=relationalExpressionNoIn { $value = new BinaryExpression(type, $value, right.value); })*
	;

// $>
		
// $<Binary bitwise operators (11.10)

bitwiseANDExpression returns [Expression value]
	: left=equalityExpression { $value = left.value; } ( AND^ right=equalityExpression { $value = new BinaryExpression(BinaryExpressionType.BitwiseAnd, $value, right.value); })*
	;

bitwiseANDExpressionNoIn returns [Expression value]
	: left=equalityExpressionNoIn { $value = left.value; } ( AND^ right=equalityExpressionNoIn { $value = new BinaryExpression(BinaryExpressionType.BitwiseAnd, $value, right.value); })*
	;
		
bitwiseXORExpression returns [Expression value]
	: left=bitwiseANDExpression { $value = left.value; } ( XOR^ right=bitwiseANDExpression { $value = new BinaryExpression(BinaryExpressionType.BitwiseXOr, $value, right.value); })*
	;
		
bitwiseXORExpressionNoIn returns [Expression value]
	: left=bitwiseANDExpressionNoIn { $value = left.value; } ( XOR^ right=bitwiseANDExpressionNoIn { $value = new BinaryExpression(BinaryExpressionType.BitwiseXOr, $value, right.value); })*
	;
	
bitwiseORExpression returns [Expression value]
	: left=bitwiseXORExpression { $value = left.value; } ( OR^ right=bitwiseXORExpression { $value = new BinaryExpression(BinaryExpressionType.BitwiseOr, $value, right.value); })*
	;
	
bitwiseORExpressionNoIn returns [Expression value]
	: left=bitwiseXORExpressionNoIn { $value = left.value; } ( OR^ right=bitwiseXORExpressionNoIn { $value = new BinaryExpression(BinaryExpressionType.BitwiseOr, $value, right.value); })*
	;

// $>
	
// $<Binary logical operators (11.11)

logicalANDExpression returns [Expression value]
	:left= bitwiseORExpression { $value = left.value; } ( LAND^ right=bitwiseORExpression { $value = new BinaryExpression(BinaryExpressionType.And, $value, right.value); })*
	;

logicalANDExpressionNoIn returns [Expression value]
	:left= bitwiseORExpressionNoIn { $value = left.value; } ( LAND^ right=bitwiseORExpressionNoIn { $value = new BinaryExpression(BinaryExpressionType.And, $value, right.value); })*
	;
	
logicalORExpression returns [Expression value]
	: left=logicalANDExpression { $value = left.value; } ( LOR^ right=logicalANDExpression { $value = new BinaryExpression(BinaryExpressionType.Or, $value, right.value); })*
	;
	
logicalORExpressionNoIn returns [Expression value]
	: left=logicalANDExpressionNoIn { $value = left.value; } ( LOR^ right=logicalANDExpressionNoIn { $value = new BinaryExpression(BinaryExpressionType.Or, $value, right.value); } )*
	;

// $>
	
// $<Conditional operator (11.12)

conditionalExpression returns [Expression value]
	: expr1=logicalORExpression { $value = expr1.value; } ( QUE^ expr2=assignmentExpression COLON! expr3=assignmentExpression { $value = new TernaryExpression(expr1.value, expr2.value, expr3.value); })?
	;

conditionalExpressionNoIn returns [Expression value]
	: expr1=logicalORExpressionNoIn { $value = expr1.value; } ( QUE^ expr2=assignmentExpressionNoIn COLON! expr3=assignmentExpressionNoIn { $value = new TernaryExpression(expr1.value, expr2.value, expr3.value); })?
	;
	
// $>

// $<Assignment operators (11.13)

/*
The specification defines the AssignmentExpression rule as follows:
AssignmentExpression :
	ConditionalExpression 
	LeftHandSideExpression AssignmentOperator AssignmentExpression
This rule has a LL(*) conflict. Resolving this with a syntactical predicate will yield something like this:

assignmentExpression
	: ( leftHandSideExpression assignmentOperator )=> leftHandSideExpression assignmentOperator^ assignmentExpression
	| conditionalExpression
	;
assignmentOperator
	: ASSIGN | MULASS | DIVASS | MODASS | ADDASS | SUBASS | SHLASS | SHRASS | SHUASS | ANDASS | XORASS | ORASS
	;
	
But that didn't seem to work. Terence Par writes in his book that LL(*) conflicts in general can best be solved with auto backtracking. But that would be 
a performance killer for such a heavy used rule.
The solution I came up with is to always invoke the conditionalExpression first and than decide what to do based on the result of that rule.
When the rule results in a Tree that can't be coming from a left hand side expression, then we're done.
When it results in a Tree that is coming from a left hand side expression and the LA(1) is an assignment operator then parse the assignment operator
followed by the right recursive call.
*/
assignmentExpression returns [Expression value]
@init
{
	Object[] isLhs = new Object[1];
	var assignment = new AssignmentExpression();
}
	: lhs=conditionalExpression { $value = assignment.Left = lhs.value; }
	(  { IsLeftHandSideAssign(lhs.value, isLhs) }? ass=assignmentOperator^ { assignment.AssignmentOperator = ResolveAssignmentOperator($ass.text); } exp=assignmentExpression {  assignment.Right = exp.value; $value = assignment; } )?
	;

assignmentOperator 
	: ASSIGN
	| MULASS
	| DIVASS
	| MODASS
	| ADDASS
	| SUBASS
	| SHLASS
	| SHRASS
	| SHUASS
	| ANDASS
	| XORASS
	| ORASS
	;
	
assignmentExpressionNoIn returns [Expression value]
@init
{
	object[] isLhs = new object[1];
	var assignment = new AssignmentExpression();
}
	: lhs=conditionalExpressionNoIn {  assignment.Left = $value = $lhs.value; } 
	( { IsLeftHandSideAssign(lhs.value, isLhs) }? ass=assignmentOperator^ { assignment.AssignmentOperator = ResolveAssignmentOperator($ass.text); } exp=assignmentExpressionNoIn {  assignment.Right = exp.value; $value = assignment; } )?
	;
	
// $>
	
// $<Comma operator (11.14)

expression returns [Expression value]
@init{
	var cs = new CommaOperatorStatement();
}
	: first=assignmentExpression { $value = first.value; } ( COMMA { if(cs.Statements.Count == 0) { cs.Statements.Add($value); $value = cs; } } follow=assignmentExpression  { cs.Statements.Add(follow.value); } )* 
	;

expressionNoIn returns [Expression value]
@init{
	var cs = new CommaOperatorStatement();
}
	: first=assignmentExpressionNoIn { $value = first.value; } ( COMMA {if(cs.Statements.Count == 0) { cs.Statements.Add($value); $value = cs; } } follow=assignmentExpressionNoIn  { cs.Statements.Add(follow.value); } )* 
	;

// $>

// $>
	
//
// $<	A.4 Statements (12)
//

/*
This rule handles semicolons reported by the lexer and situations where the ECMA 3 specification states there should be semicolons automaticly inserted.
The auto semicolons are not actually inserted but this rule behaves as if they were.

In the following situations an ECMA 3 parser should auto insert absent but grammaticly required semicolons:
- the current token is a right brace
- the current token is the end of file (EOF) token
- there is at least one end of line (EOL) token between the current token and the previous token.

The RBRACE is handled by matching it but not consuming it.
The EOF needs no further handling because it is not consumed by default.
The EOL situation is handled by promoting the EOL or MultiLineComment with an EOL present from off channel to on channel
and thus making it parseable instead of handling it as white space. This promoting is done in the action PromoteEOL.
*/
semic
@init
{
	// Mark current position so we can unconsume a RBRACE.
	int marker = input.Mark();
	// Promote EOL if appropriate	
	PromoteEOL(retval);
}
	: SEMIC
	| EOF
	| RBRACE { input.Rewind(marker); }
	| EOL | MultiLineComment // (with EOL in it)
	;

/*
To solve the ambiguity between block and objectLiteral via expressionStatement all but the block alternatives have been moved to statementTail.
Now when k = 1 and a semantical predicate is defined ANTLR generates code that always will prefer block when the LA(1) is a LBRACE.
This will result in the same behaviour that is described in the specification under 12.4 on the expressionStatement rule.
*/
statement returns [Statement value]
options
{
	k = 1 ;
}

	: { input.LA(1) == LBRACE }? block { $value = $block.value; }
	| { input.LA(1) == FUNCTION }? func=functionDeclaration { $value = func.value; }
	| statementTail { $value = $statementTail.value; }
	;
	
statementTail returns [Statement value] 
@after{
        if (!(retval.value is ForStatement ||
            retval.value is BlockStatement ||
            retval.value is WhileStatement ||
            retval.value is DoWhileStatement ||
            retval.value is SwitchStatement ||
            retval.value is TryStatement ||
            retval.value is IfStatement)) {
            retval.value.Source = ExtractSourceCode((CommonToken)retval.Start, (CommonToken)retval.Stop);
        }
}
	: variableStatement { $value = $variableStatement.value; }
	| emptyStatement { $value = $emptyStatement.value; }
	| expressionStatement { $value = $expressionStatement.value; }
	| ifStatement { $value = $ifStatement.value; }
	| iterationStatement { $value = $iterationStatement.value; }
	| continueStatement { $value = $continueStatement.value; }
	| breakStatement { $value = $breakStatement.value; }
	| returnStatement { $value = $returnStatement.value; }
	| withStatement { $value = $withStatement.value; }
	| labelledStatement { $value = $labelledStatement.value; }
	| switchStatement { $value = $switchStatement.value; }
	| throwStatement { $value = $throwStatement.value; }
	| tryStatement { $value = $tryStatement.value; }
	;

// $<Block (12.1)

block returns [BlockStatement value] 
@init{
	$value = new BlockStatement();
}
@after{
	$value.Source = ExtractSourceCode((CommonToken)retval.Start, (CommonToken)retval.Stop);
}
	: lb=LBRACE (statement { $value.Statements.AddLast($statement.value); })* RBRACE
	
	;

// $>
	
// $<Variable statement 12.2)

variableStatement returns [Statement value]
@init{
	var cs = new CommaOperatorStatement();
}
@after{
	// hoisting
	if(cs.Statements.Count > 0) {
		foreach(var vd in cs.Statements) {
			var nvd = new VariableDeclarationStatement();
			nvd.Global = false;
			nvd.Identifier = ((VariableDeclarationStatement)vd).Identifier;
			_currentBody.AddFirst(nvd);
		}
	}
	else {
		var nvd = new VariableDeclarationStatement();
		nvd.Global = false;
		nvd.Identifier = first.value.Identifier;
		_currentBody.AddFirst(nvd);
	}
}
	: VAR first=variableDeclaration { first.value.Global = false; $value = first.value; } ( COMMA { if( cs.Statements.Count == 0) { cs.Statements.Add($value); $value = cs; } } follow=variableDeclaration  { cs.Statements.Add(follow.value); follow.value.Global = false; } )* semic
	
	;

variableDeclaration returns [VariableDeclarationStatement value]
@init {
	$value = new VariableDeclarationStatement();
	$value.Global = true;
}
	: id=Identifier { $value.Identifier = id.Text; } ( ASSIGN^ ass=assignmentExpression { $value.Expression = ass.value; } )?
	;
	
variableDeclarationNoIn returns [VariableDeclarationStatement value]
@init {
	$value = new VariableDeclarationStatement();
	$value.Global = true;
}
	: id=Identifier { $value.Identifier = id.Text; } ( ASSIGN^ ass=assignmentExpressionNoIn { $value.Expression = ass.value; } )?
	;

// $>
	
// $<Empty statement (12.3)

emptyStatement returns [Statement value]
	: SEMIC! { $value = new EmptyStatement(); }
	;

// $>
	
// $<Expression statement (12.4)

/*
The look ahead check on LBRACE and FUNCTION the specification mentions has been left out and its function, resolving the ambiguity between:
- functionExpression and functionDeclaration
- block and objectLiteral
are moved to the statement and sourceElement rules.
*/
expressionStatement returns [Statement value]
	: expression semic! { $value = new ExpressionStatement($expression.value); }
	;

// $>
	
// $<The if statement (12.5)

ifStatement returns [Statement value]
@init {
var st = new IfStatement();
$value = st;
}
// The predicate is there just to get rid of the warning. ANTLR will handle the dangling else just fine.
	: IF LPAREN expression { st.Expression = $expression.value; } RPAREN then=statement { st.Then = $then.value; } ( { input.LA(1) == ELSE }? ELSE els=statement { st.Else = $els.value; } )?
	

	;

// $>
	
// $<Iteration statements (12.6)

iterationStatement returns [Statement value]
	: dos=doStatement { $value = dos.value; }
	| wh=whileStatement  { $value = wh.value; }
	| fo=forStatement  { $value = (Statement)fo.value; }
	;
	
doStatement returns [Statement value]
	: DO statement WHILE LPAREN expression RPAREN semic { $value = new DoWhileStatement($expression.value, $statement.value); }
	
	;
	
whileStatement returns [Statement value]
	: WHILE^ LPAREN! expression RPAREN! statement { $value = new WhileStatement($expression.value, $statement.value); }
	;

/*
The forStatement production is refactored considerably as the specification contains a very none LL(*) compliant definition.
The initial version was like this:	

forStatement
	: FOR^ LPAREN! forControl RPAREN! statement
	;
forControl
options
{
	backtrack = true ;
	//k = 3 ;
}
	: stepClause
	| iterationClause
	;
stepClause
options
{
	memoize = true ;
}
	: ( ex1=expressionNoIn | var=VAR variableDeclarationNoIn ( COMMA variableDeclarationNoIn )* )? SEMIC ex2=expression? SEMIC ex3=expression?
	-> { $var != null }? ^( FORSTEP ^( VAR[$var] variableDeclarationNoIn+ ) ^( EXPR $ex2? ) ^( EXPR $ex3? ) )
	-> ^( FORSTEP ^( EXPR $ex1? ) ^( EXPR $ex2? ) ^( EXPR $ex3? ) )
	;
iterationClause
options
{
	memoize = true ;
}
	: ( leftHandSideExpression | var=VAR variableDeclarationNoIn ) IN expression
	-> { $var != null }? ^( FORITER ^( VAR[$var] variableDeclarationNoIn ) ^( EXPR expression ) )
	-> ^( FORITER ^( EXPR leftHandSideExpression ) ^( EXPR expression ) )
	;
	
But this completely relies on the backtrack feature and capabilities of ANTLR. 
Furthermore backtracking seemed to have 3 major drawbacks:
- the performance cost of backtracking is considerably
- didn't seem to work well with ANTLRWorks
- when introducing a k value to optimize the backtracking away, ANTLR runs out of heap space
*/
forStatement returns [IForStatement value]
	: FOR^ LPAREN! fo=forControl { $value = fo.value; }  RPAREN! st=statement {  $value.Statement = st.value; }
	;

forControl returns [IForStatement value]
	: ex1=forControlVar { $value = ex1.value; }
	| ex2=forControlExpression { $value = ex2.value; }
	| ex3=forControlSemic { $value = ex3.value; }
	;

forControlVar returns [IForStatement value]
@init {
	var forStatement = new ForStatement();
	var foreachStatement = new ForEachInStatement();
	var cs = new CommaOperatorStatement();
}
@after {
	// hoisting
	if(cs.Statements.Count > 0) {
		foreach(var vd in cs.Statements) {
			var nvd = new VariableDeclarationStatement();
			nvd.Global = false;
			nvd.Identifier = ((VariableDeclarationStatement)vd).Identifier;
			_currentBody.AddFirst(nvd);
		}
	}
	else {
		var nvd = new VariableDeclarationStatement();
		nvd.Global = false;
		nvd.Identifier = first.value.Identifier;
		_currentBody.AddFirst(nvd);
	}
}

	: VAR first=variableDeclarationNoIn { foreachStatement.InitialisationStatement = forStatement.InitialisationStatement = first.value; first.value.Global = false;  }
	(
		(
			IN ex=expression { $value = foreachStatement; foreachStatement.Expression = $ex.value; }
			
		)
		|
		(
			( COMMA { if( cs.Statements.Count == 0) { foreachStatement.InitialisationStatement = forStatement.InitialisationStatement = cs; cs.Statements.Add(first.value); } } follow=variableDeclarationNoIn {  follow.value.Global = false; cs.Statements.Add(follow.value); } )* 
			SEMIC ( ex1=expression { forStatement.ConditionExpression = $ex1.value;} ) ? SEMIC (ex2=expression {  forStatement.IncrementExpression = $ex2.value; })? { $value = forStatement; }
			
		)
	)
	;

forControlExpression returns [IForStatement value]
@init
{
	var forStatement = new ForStatement();
	var foreachStatement = new ForEachInStatement();

	object[] isLhs = new object[1];
}
	: ex1=expressionNoIn { foreachStatement.InitialisationStatement = forStatement.InitialisationStatement = ex1.value; }
	( 
		{ IsLeftHandSideIn(ex1.value, isLhs) }? (
			IN ex2=expression { $value = foreachStatement; foreachStatement.Expression = ex2.value; }
			
		)
		|
		(
			SEMIC ( ex2=expression { forStatement.ConditionExpression = ex2.value;} ) ? SEMIC (ex3=expression {  forStatement.IncrementExpression = ex3.value; })? { $value = forStatement; }
			
		)
	)
	;

forControlSemic returns [ForStatement value]
@init{
	$value = new ForStatement();
}
	: SEMIC ( ex1=expression { $value.ConditionExpression = ex1.value;} ) ? SEMIC (ex2=expression {  $value.IncrementExpression = ex2.value; })? 
	
	;

// $>
	
// $<The continue statement (12.7)

/*
The action with the call to PromoteEOL after CONTINUE is to enforce the semicolon insertion rule of the specification that there are
no line terminators allowed beween CONTINUE and the optional identifier.
As an optimization we check the la first to decide whether there is an identier following.
*/
continueStatement returns [Statement value]
@init { 
	string label = String.Empty; 
}
	: CONTINUE^ { if (input.LA(1) == Identifier) PromoteEOL(null); } (lb=Identifier { label = lb.Text; } )? semic! { $value = new ContinueStatement() { Label = label }; }
	;

// $>
	
// $<The break statement (12.8)

/*
The action with the call to PromoteEOL after BREAK is to enforce the semicolon insertion rule of the specification that there are
no line terminators allowed beween BREAK and the optional identifier.
As an optimization we check the la first to decide whether there is an identier following.
*/
breakStatement returns [Statement value]
@init { 
	string label = String.Empty; 
}
	: BREAK^ { if (input.LA(1) == Identifier) PromoteEOL(null); } (lb=Identifier { label = lb.Text; } )? semic! { $value = new BreakStatement() { Label = label }; }
	;

// $>
	
// $<The return statement (12.9)

/*
The action calling PromoteEOL after RETURN ensures that there are no line terminators between RETURN and the optional expression as the specification states.
When there are these get promoted to on channel and thus virtual semicolon wannabees.
So the folowing code:

return
1

will be parsed as:

return;
1;
*/
returnStatement returns [ReturnStatement value]
@init {
	$value = new ReturnStatement();
}
	: RETURN^ { PromoteEOL(null); } (expr=expression { $value.Expression = expr.value; })? semic!
	;

// $>
	
// $<The with statement (12.10)

withStatement returns [Statement value]
	: WITH^ LPAREN! exp=expression RPAREN! smt=statement { $value = new WithStatement(exp.value, smt.value); }
	;

// $>
	
// $<The switch statement (12.11)

switchStatement returns [Statement value]
@init {
	SwitchStatement switchStatement = new SwitchStatement();
	$value = switchStatement;
	int defaultClauseCount = 0;
}
	:	SWITCH LPAREN expression { switchStatement.Expression = $expression.value; } RPAREN 
		LBRACE ( { defaultClauseCount == 0 }?=> defaultClause { defaultClauseCount++; switchStatement.DefaultStatements=$defaultClause.value; } | caseClause { switchStatement.CaseClauses.Add($caseClause.value); } )* RBRACE
		
	;

caseClause returns [CaseClause value]
@init {
	$value = new CaseClause();
}
	: CASE^ expression { $value.Expression = $expression.value; } COLON!( statement { $value.Statements.Statements.AddLast($statement.value); })*
	;
	
defaultClause returns [BlockStatement value]
@init {
	$value = new BlockStatement();
}
	: DEFAULT^ COLON! (statement { $value.Statements.AddLast($statement.value); }) *
	;

// $>
	
// $<Labelled statements (12.12)

labelledStatement returns [Statement value]
	: lb=Identifier COLON st=statement { $value = st.value;  $value.Label = lb.Text; }
	
	;

// $>
	
// $<The throw statement (12.13)

/*
The action calling PromoteEOL after THROW ensures that there are no line terminators between THROW and the expression as the specification states.
When there are line terminators these get promoted to on channel and thus to virtual semicolon wannabees.
So the folowing code:

throw
new Error()

will be parsed as:

throw;
new Error();

which will yield a recognition exception!
*/
throwStatement returns [Statement value]
	: THROW^ { PromoteEOL(null); } exp=expression { $value = new ThrowStatement(exp.value); } semic!
	;

// $>
	
// $<The try statement (12.14)

tryStatement returns [TryStatement value]
@init{
	$value = new TryStatement();
}
	: TRY^ b=block  { $value.Statement = b.value; } ( c=catchClause { $value.Catch = c.value; } (first=finallyClause { $value.Finally = first.value; })? | last=finallyClause { $value.Finally = last.value; } )
	;
	
catchClause returns [CatchClause value]
	: CATCH^ LPAREN! id=Identifier RPAREN! block { $value = new CatchClause($id.text, $block.value); }
	;
	
finallyClause returns [FinallyClause value]
	: FINALLY^ block { $value = new FinallyClause($block.value); }
	;

// $>

// $>

//
// $<	A.5 Functions and Programs (13, 14)
//

// $<	Function Definition (13)

functionDeclaration returns [Statement value]
@init {
FunctionDeclarationStatement statement = new FunctionDeclarationStatement();
$value = new EmptyStatement();
_currentBody.AddFirst(statement);
}
	: FUNCTION 	name=Identifier { statement.Name = name.Text; } 
			parameters=formalParameterList { statement.Parameters.AddRange(parameters.value); }
			body=functionBody { statement.Statement = body.value; }
	  

	;

functionExpression returns [FunctionExpression value]
@init {
	$value = new FunctionExpression();
}
	: FUNCTION (name=Identifier { $value.Name = name.Text; } )? formalParameterList { $value.Parameters.AddRange($formalParameterList.value) ;} functionBody { $value.Statement = $functionBody.value; }
	

	;

formalParameterList returns [List<string> value]
@init {
List<string> identifiers = new List<string>();
$value = identifiers;
}
	: LPAREN ( first=Identifier { identifiers.Add($first.text); } ( COMMA follow=Identifier  { identifiers.Add($follow.text); } )* )? RPAREN
	
	;

functionBody returns [BlockStatement value]
@init{
BlockStatement block = new BlockStatement();
var tempBody = _currentBody;
_currentBody = block.Statements;
$value = block;
}
@after{
_currentBody = tempBody;
}
	: lb=LBRACE (sourceElement { block.Statements.AddLast($sourceElement.value); }) * RBRACE
	
	;

// $>
	
// $<	Program (14)

program returns [Program value]
@init{
script = input.ToString().Split('\n');
Program program = new Program();
_currentBody = program.Statements;
}
	: (follow=sourceElement { program.Statements.AddLast(follow.value); })* { $value = program; }
	;

/*
By setting k  to 1 for this rule and adding the semantical predicate ANTRL will generate code that will always prefer functionDeclararion over functionExpression
here and therefor remove the ambiguity between these to production.
This will result in the same behaviour that is described in the specification under 12.4 on the expressionStatement rule.
*/
sourceElement returns [Statement value]
options
{
	k = 1 ;
}

	: { input.LA(1) == FUNCTION }? func=functionDeclaration { $value = func.value; }
	| stat=statement { $value = stat.value; }
	;

// $>

// $>

