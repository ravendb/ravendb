%namespace Raven.Database.Indexing
%scannertype LuceneQueryScanner
%visibility internal
%tokentype Token

%option stack, minimize, parser, verbose, persistbuffer, noembedbuffers 

Comment    [ \t\r\n\f]"//"([^\n\r]*)
Whitespace [ \t\r\n\f]
Digit      [0-9]
Number     {Digit}+
Decimal    {Number}\.{Number}
EscapeChar \\[^]
TermStartChar [^ :\t\r\n\f\+\-!\{\}()"^\?\\~\[\],]|{EscapeChar}
TermChar {TermStartChar}|[,\-\+]
QuotedChar [^\"\\]|{EscapeChar}
UnanalizedTerm \[\[(([^\]])|([\]][^\]]+))*\]\]
QuotedTerm \"{QuotedChar}*\"
UnquotedTerm {TermStartChar}{TermChar}*
Method \@[^<]+\<[^>]+\>
DateTime {Digit}{4}-{Digit}{2}-{Digit}{2}T{Digit}{2}\:{Digit}{2}\:{Digit}{2}\.{Digit}{7}Z?

%{

%}

%%
","				 {return (int)Token.COMMA;}
"^"				 {return (int)Token.BOOST;}
"~"				 {return (int)Token.TILDA;}
"{"				 {return (int)Token.OPEN_CURLY_BRACKET;}
"}"				 {return (int)Token.CLOSE_CURLY_BRACKET;}
"["				 {return (int)Token.OPEN_SQUARE_BRACKET;}
"]"				 {return (int)Token.CLOSE_SQUARE_BRACKET;}
"TO"			 {return (int)Token.TO;}
"OR"			 {return (int)Token.OR;}
"||"			 {return (int)Token.OR;}
"AND"			 {return (int)Token.AND;}
"&&"			 {return (int)Token.AND;}
"NOT"			 {return (int)Token.NOT;}
"+"				 {return (int)Token.PLUS;}
"-"				 {return (int)Token.MINUS;}
"\""			 {return (int)Token.QUOTE;}
":"				 {return (int)Token.COLON;}
"("				 {return (int)Token.OPEN_PAREN;}
")"				 {return (int)Token.CLOSE_PAREN;}
"INTERSECT"		 {return (int)Token.INTERSECT;}
{DateTime}		 { yylval.s = yytext; return (int)Token.DATETIME;}
{Method}         { yylval.s = yytext; return (int)Token.METHOD;}
{UnanalizedTerm} { yylval.s = yytext; return (int)Token.UNANALIZED_TERM;}
{QuotedTerm}	 { yylval.s = yytext; return (int)Token.QUOTED_TERM;}
{Comment}        {/* skip */}
{Decimal}		 { yylval.s = yytext; return (int)Token.FLOAT_NUMBER;}
{Number}		 { yylval.s = yytext; return (int)Token.INT_NUMBER;}
{UnquotedTerm}   { 					
					if(InMethod && bStack.Count == 0) 
					{
						yylval.s = HandleTermInMethod();
					}
					else 
					{
						yylval.s = yytext;
					}
					return (int)Token.UNQUOTED_TERM;
				 }
{Whitespace}	 {/* skip */}
<<EOF>>			 /*This is needed for yywrap to work, do not delete this comment!!!*/

%%