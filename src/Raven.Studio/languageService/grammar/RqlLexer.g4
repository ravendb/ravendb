lexer grammar RqlLexer;

//SPECIAL CHARACTERS:
CL_CUR:     '}';
CL_PAR:     ')';   
CL_Q:       ']';
COMMA:      ',';
DOT:        '.';
D_QUOTE:    '"';
EQUAL:      '=' | '==' | '<>' | '!=';
MATH:       '<' | '>'  | '<=' | '>=';
OP_CUR:     '{';
OP_PAR:     '(';
OP_Q:       '[';
SLASH:      '/';
COLON:		':';
SEMICOLON:	';';
BACKSLASH:	[\\];
PLUS:		'+';
MINUS:		'-';
AT:			'@';
HASH:		'#';
DOL:		'$'; 
PERCENT:    '%';
POWER:		'^';
AMP:		'&';
STAR:		'*';
QUESTION_MARK:	'?';
EXCLAMATION:	'!';
//RQL keywords
ALL:            A L L;
ALL_DOCS:       '@all_docs';
ALPHANUMERIC:   A L P H A N U M E R I C;
AND:            A N D;
AS:             A S;
BETWEEN:        B E T W E E N;
DECLARE:        D E C L A R E;
DISTINCT:       D I S T I N C T;
DOUBLE:         D O U B L E;
ENDS_WITH:      E N D S W I T H;
STARTS_WITH:	S T A R T S W I T H;
FALSE:          F A L S E;
FACET:			F A C E T;
FROM:           F R O M;
GROUP_BY:       G R O U P ' ' B Y;
ID:             I D ;
IN:             I N;
INCLUDE:        I N C L U D E;
INDEX:          I N D E X;
INTERSECT:      I N T E R S E C T;
LOAD:           L O A D;
LONG:           L O N G;
MATCH:          M A T C H;
METADATA:       AT M E T A D A T A;
MORELIKETHIS:   M O R E L I K E T H I S;
NOT:            N O T;
NULL:           N U L L;
OR:             O R;
ORDER_BY:       O R D E R ' ' B Y;
OFFSET:         O F F S E T;
SELECT:         S E L E C T;
SORTING:        A S C | A S C E N D I N G | D E S C | D E S C E N D I N G;
STRING_W:       S T R I N G;
TO:             T O;
TRUE:           T R U E;
WHERE:          W H E R E;
WITH:           W I T H;
EXACT:			E X A C T;
BOOST:			B O O S T;
SEARCH:         S E A R C H;
LIMIT:          L I M I T;
FUZZY:          F U Z Z Y;
RQLJS:          AT R Q L J S;
JAVASCRIPT: ('{' ( JAVASCRIPT | ~'{'  | ~'}' )*? '}')   -> channel(3);
//Literals
JS_FUNCTION_DECLARATION: 'declare function';
NUM: DIGIT+ (DOT DIGIT+)?;
STRING: SINGLE_QUOTE_STRING
        | ('"' ( '\\"' | . )*? '"' ) | ('\'' ( '\\"' | . )*?  '\'' )
        | '"' UTFEscape '"' 
        | '\'' UTFEscape '\''
        ;
SINGLE_QUOTE_STRING: '\'' ( ('\'\'') | ('\\'+ ~'\\') | ~('\'' | '\\') )* '\'' ;

WORD: AT? [a-zA-Z_0-9-]+;


// fragments
fragment UTFEscape: '\\u' HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT
                    | '\\U' HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT
                    ;
fragment HEXDIGIT: [0-9] | [A-F] | [a-f];
fragment DIGIT:     [0-9];
fragment A :    [aA];
fragment B :    [bB];
fragment C :    [cC];
fragment D :    [dD];
fragment E :    [eE];
fragment F :    [fF];
fragment G :    [gG];
fragment H :    [hH];
fragment I :    [iI]; 
fragment J :    [jJ];
fragment K :    [kK];
fragment L :    [lL];
fragment M :    [mM];
fragment N :    [nN];
fragment O :    [oO];
fragment P :    [pP];
fragment Q :    [qQ];
fragment R :    [rR];
fragment S :    [sS];
fragment T :    [tT];
fragment U :    [uU];
fragment V :    [vV];
fragment W :    [wW];
fragment X :    [xX];
fragment Y :    [yY];
fragment Z :    [zZ];

WS: [ \n\t\r]+ -> channel(HIDDEN);
