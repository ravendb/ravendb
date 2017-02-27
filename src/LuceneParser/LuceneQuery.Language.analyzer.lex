%namespace Raven.Server.Documents.Queries.Parse
%scannertype LuceneQueryScanner
%visibility internal
%tokentype Token

%option stack, minimize, parser, verbose, persistbuffer, noembedbuffers 

Eol             (\r\n?|\n)
NotWh           [^ \t\r\n]
Space           [ \t]
Number          [0-9]+

%{

%}

%%

/* Scanner body */

{Number}		{ Console.WriteLine("token: {0}", yytext);		GetNumber(); return (int)Token.NUMBER; }

{Space}+		/* skip */


%%