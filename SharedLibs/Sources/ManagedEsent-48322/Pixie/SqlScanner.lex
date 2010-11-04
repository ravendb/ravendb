%namespace Microsoft.Isam.Esent.Sql.Parsing

%{
     public override void yyerror(string format, params object[] args)
     {
		throw new EsentSqlParseException(String.Format(format, args));
     }
%}

%%

ATTACH		{ return (int)Tokens.ATTACH; }
BEGIN		{ return (int)Tokens.BEGIN; }
BINARY		{ return (int)Tokens.BINARY; }
BOOL(EAN)?	{ return (int)Tokens.BOOL; }
BYTE		{ return (int)Tokens.BYTE; }
COMMIT		{ return (int)Tokens.COMMIT; }
CREATE		{ return (int)Tokens.CREATE; }
DATABASE	{ return (int)Tokens.DATABASE; }
DATETIME	{ return (int)Tokens.DATETIME; }
DETACH		{ return (int)Tokens.DETACH; }
END			{ return (int)Tokens.END; }
GUID		{ return (int)Tokens.GUID; }
INDEX		{ return (int)Tokens.INDEX; }
INSERT		{ return (int)Tokens.INSERT; }
INT(EGER)?	{ return (int)Tokens.INT; }
INTO		{ return (int)Tokens.INTO; }
LONG		{ return (int)Tokens.LONG; }
RELEASE		{ return (int)Tokens.RELEASE; }
ROLLBACK	{ return (int)Tokens.ROLLBACK; }
SAVEPOINT	{ return (int)Tokens.SAVEPOINT; }
SHORT		{ return (int)Tokens.SHORT; }
TABLE		{ return (int)Tokens.TABLE; }
TEXT		{ return (int)Tokens.TEXT; }
TO			{ return (int)Tokens.TO; }
TRANSACTION	{ return (int)Tokens.TRANSACTION; }
VALUES		{ return (int)Tokens.VALUES; }

[A-Za-z][A-Za-z0-9_]*	{ yylval.name = yytext; return (int)Tokens.NAME; }

[\-+*/:(),.;]			{ return yytext[0]; }

[0-9]+ 					{ yylval.intValue = Int64.Parse(yytext); return (int)Tokens.INTEGER; }

[0-9]+"."[0-9]* |
"."[0-9]+ 				{ yylval.realValue = Double.Parse(yytext); return (int)Tokens.REAL_NUMBER; }

"--".* ; /* SQL Comment */

'[^']*'					{ yylval.stringValue = yytext.Substring(1,yytext.Length-2); return (int)Tokens.STRING; }

