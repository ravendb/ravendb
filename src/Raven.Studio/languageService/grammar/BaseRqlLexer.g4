lexer grammar BaseRqlLexer;
//SPECIAL CHARACTERS:

CL_CUR
   : '}'
   ;

CL_PAR
   : ')'
   ;

CL_Q
   : ']'
   ;

COMMA
   : ','
   ;

DOT
   : '.'
   ;

EQUAL
   : '='
   | '=='
   | '<>'
   | '!='
   ;

MATH
   : '<'
   | '>'
   | '<='
   | '>='
   ;

OP_CUR
   : '{'
   ;

OP_PAR
   : '('
   ;

OP_Q
   : '['
   ;

SLASH
   : '/'
   ;

COLON
   : ':'
   ;

SEMICOLON
   : ';'
   ;

BACKSLASH
   : [\\]
   ;

PLUS
   : '+'
   ;

MINUS
   : '-'
   ;

AT
   : '@'
   ;

HASH
   : '#'
   ;

DOL
   : '$'
   ;

PERCENT
   : '%'
   ;

POWER
   : '^'
   ;

AMP
   : '&'
   ;

STAR
   : '*'
   ;

QUESTION_MARK
   : '?'
   ;

EXCLAMATION
   : '!'
   ;
   //RQL keywords
   
ALL
   : A L L
   ;

ALL_DOCS
   : '@all_docs'
   ;

ALPHANUMERIC
   : A L P H A N U M E R I C
   ;

AND
   : A N D
   ;

AS
   : A S
   ;

BETWEEN
   : B E T W E E N
   ;

DISTINCT
   : D I S T I N C T
   ;

DOUBLE
   : D O U B L E
   ;

ENDS_WITH
   : E N D S W I T H
   ;

STARTS_WITH
   : S T A R T S W I T H
   ;

FALSE
   : F A L S E
   ;

FACET
   : F A C E T
   ;

FROM
   : F R O M
   ;

GROUP_BY
   : G R O U P ' ' B Y
   ;

ID
   : I D
   ;

IN
   : I N
   ;

INCLUDE
   : I N C L U D E
   ;

UPDATE
   : U P D A T E -> pushMode (UPDATE_STATEMENT)
   ;

INDEX
   : I N D E X
   ;

INTERSECT
   : I N T E R S E C T
   ;

LOAD
   : L O A D
   ;

LONG
   : L O N G
   ;

MATCH
   : M A T C H
   ;

METADATA
   : AT M E T A D A T A
   ;

MORELIKETHIS
   : M O R E L I K E T H I S
   ;

NOT
   : N O T
   ;

NULL
   : N U L L
   ;

OR
   : O R
   ;

ORDER_BY
   : O R D E R ' ' B Y
   ;

OFFSET
   : O F F S E T
   ;

SELECT
   : S E L E C T
   ;

JS_SELECT
   : S E L E C T ' '* OP_CUR -> pushMode (JAVASCRIPT_STATEMENT)
   ;

SORTING
   : A S C
   | A S C E N D I N G
   | D E S C
   | D E S C E N D I N G
   ;

STRING_W
   : S T R I N G
   ;

TO
   : T O
   ;

TRUE
   : T R U E
   ;

WHERE
   : W H E R E
   ;

WITH
   : W I T H
   ;

EXACT
   : E X A C T
   ;

BOOST
   : B O O S T
   ;

SEARCH
   : S E A R C H
   ;

LIMIT
   : L I M I T
   ;

FUZZY
   : F U Z Z Y
   ;

FILTER
   : F I L T E R
   ;
   
FILTER_LIMIT
   : F I L T E R '_' L I M I T
   ;

TIMESERIES
   : T I M E S E R I E S ' '? OP_PAR -> pushMode (TIME_SERIES)
   ;
   //JAVASCRIPT
   
   //   : ('{' (JAVASCRIPT | ~ '{' | ~ '}')*? '}') -> channel (3)
   
   //   ;
   
   //Literals
   
JS_FUNCTION_DECLARATION
   : D E C L A R E ' ' F U N C T I O N -> pushMode (JAVASCRIPT_STATEMENT) , pushMode (JAVASCRIPT_FUNCTION_NAME)
   ;

TIMESERIES_FUNCTION_DECLARATION
   : D E C L A R E ' ' T I M E S E R I E S -> pushMode (TIME_SERIES)
   ;

NUM
   : DIGIT+ (DOT DIGIT+)?
   ;

DOUBLE_QUOTE_STRING
   : '"' ('\\"' | .)*? '"'
   | '"' UTFEscape '"'
   ;

SINGLE_QUOTE_STRING
   : '\'' (('\'\'') | ('\\'+ ~ '\\') | ~ ('\'' | '\\'))* '\''
   | ('\'' ('\\"' | .)*? '\'')
   | '\'' UTFEscape '\''
   ;

WORD
   : AT? [a-zA-Z_0-9-]+
   ;

WS
   : [ \n\t\r]+ -> channel (HIDDEN)
   ;

mode TIME_SERIES;
TS_METHOD
   : M I N '()'
   | M A X '()'
   | S U M '()'
   | A V E R A G E '()'
   | A V G '()'
   | F I R S T '()'
   | L A S T '()'
   | C O U N T '()'
   | P E R C E N T I L E '(' NUM ')'
   | S L O P E '()'
   | S T A N D A R D D E V I A T I O N '()'
   ;

TS_OP_C
   : '{'
   ;

TS_CL_C
   : '}' -> popMode
   ;

TS_OP_PAR
   : '(' -> pushMode (TIME_SERIES)
   ;

TS_CL_PAR
   : ')' -> popMode
   ;

TS_OP_Q
   : '['
   ;

TS_CL_Q
   : ']'
   ;

TS_DOT
   : '.'
   ;

TS_COMMA
   : ','
   ;

TS_DOL
   : '$'
   ;

TS_MATH
   : '='
   | '=='
   | '!='
   | '<'
   | '>'
   | '<='
   | '>='
   ;

TS_OR
   : O R
   ;

TS_TRUE
   : T R U E
   ;

TS_NOT
   : N O T
   ;

TS_AS
   : A S
   ;

TS_AND
   : A N D
   ;

TS_FROM
   : F R O M
   ;

TS_WHERE
   : W H E R E
   ;

TS_GROUPBY
   : G R O U P ' ' B Y
   ;

TS_BETWEEN
   : B E T W E E N
   ;

TS_FIRST
   : F I R S T
   ;

TS_LAST
   : L A S T
   ;

TS_WITH
   : W I T H ' ' I N T E R P O L A T I O N '(' (L I N E A R | N E A R E S T | N E X T | L A S T) ')'
   ;

TS_TIMERANGE
   : S E C O N D S?
   | M I N U T E S?
   | H O U R S?
   | D A Y S?
   | M O N T H S?
   | Q U A R T E R S?
   | Y E A R S?
   ;

TS_GROUPBY_VALUE
   : '\'' NUM TS_TIMERANGE '\''
   ;

TS_SELECT
   : S E L E C T
   ;

TS_LOAD
   : L O A D ' ' T A G
   ;

TS_SCALE
   : S C A L E
   ;

TS_OFFSET
   : O F F S E T
   ;

TS_NUM
   : DIGIT+ (DOT DIGIT+)?
   ;

TS_STRING
   : TS_SINGLE_QUOTE_STRING
   | ('"' ('\\"' | .)*? '"')
   | ('\'' ('\\"' | .)*? '\'')
   | '"' UTFEscape '"'
   | '\'' UTFEscape '\''
   ;

TS_SINGLE_QUOTE_STRING
   : '\'' (('\'\'') | ('\\'+ ~ '\\') | ~ ('\'' | '\\'))* '\''
   ;

TS_WORD
   : AT? [a-zA-Z_0-9-]+
   ;

TS_WS
   : [ \n\t\r]+ -> channel (HIDDEN)
   ;

mode UPDATE_STATEMENT;
US_OP
   : '{'
   ;

US_CL
   : '}'
   ;

US_WS
   : [ \n\t\r] -> channel(HIDDEN)
   ;   
    
US_DATA
   : .+? -> channel(3)
   ;
   
mode JAVASCRIPT_STATEMENT;
JS_OP
   : '{' -> pushMode (JAVASCRIPT_STATEMENT)
   ;

JS_CL
   : '}' -> popMode
   ;

JS_DATA
   : .+? -> channel (3)
   ;

mode JAVASCRIPT_FUNCTION_NAME;
JFN_WORD
   : AT? '$'? [a-zA-Z_0-9-]+
   ;

JFN_OP_PAR
   : '('
   ;

JFN_CL_PAR
   : ')'
   ;

JFN_OP_JS
   : '{' -> popMode
   ;

JFN_COMMA
   : ','
   ;

JFN_WS
   : [ \n\t\r]+ -> channel (HIDDEN)
   ;

   // fragments
   
fragment UTFEscape
   : '\\u' HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT
   | '\\U' HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT HEXDIGIT
   ;

fragment HEXDIGIT
   : [0-9]
   | [A-F]
   | [a-f]
   ;

fragment DIGIT
   : [0-9]
   ;

fragment A
   : [aA]
   ;

fragment B
   : [bB]
   ;

fragment C
   : [cC]
   ;

fragment D
   : [dD]
   ;

fragment E
   : [eE]
   ;

fragment F
   : [fF]
   ;

fragment G
   : [gG]
   ;

fragment H
   : [hH]
   ;

fragment I
   : [iI]
   ;

fragment J
   : [jJ]
   ;

fragment K
   : [kK]
   ;

fragment L
   : [lL]
   ;

fragment M
   : [mM]
   ;

fragment N
   : [nN]
   ;

fragment O
   : [oO]
   ;

fragment P
   : [pP]
   ;

fragment Q
   : [qQ]
   ;

fragment R
   : [rR]
   ;

fragment S
   : [sS]
   ;

fragment T
   : [tT]
   ;

fragment U
   : [uU]
   ;

fragment V
   : [vV]
   ;

fragment W
   : [wW]
   ;

fragment X
   : [xX]
   ;

fragment Y
   : [yY]
   ;

fragment Z
   : [zZ]
   ;
