parser grammar RqlParser;
options { tokenVocab = RqlLexer; }

prog:
    (jsFunction)* 
    fromStatement 
    loadStatement? 
    groupByStatement?
    whereStatement? 
    loadStatement? 
    groupByStatement?
    orderByStatement? 
    selectStatement? 
    limitStatement?
    includeStatement?
    limitStatement?
    EOF
    ;

fromStatement:
    FROM INDEX indexName fromAlias? #CollectionByIndex
    | FROM ALL_DOCS #AllCollections
    | FROM collectionName fromAlias? #CollectionByName 
    ;

indexName:
    STRING
    | WORD;

loadStatement:
    LOAD 
    loadDocumentsByName
    (COMMA loadDocumentsByName)*
    ;

loadDocumentsByName:
    variable alias;

selectStatement:
    //  Select only individual fields e.g. "select Column1, Column2"
     SELECT DISTINCT STAR #getAllDistinct
    | SELECT DISTINCT?
        projectField
    (
        COMMA projectField
    )* 
    #ProjectIndividualFields
    | SELECT limitStatement? #javascriptCode
    ;
    
projectField:
    (
        parameter
        |specialFunctions
    )
    alias?
    ;
    
jsFunction: 
    (JS_FUNCTION_DECLARATION variable OP_PAR WORD (COMMA variable)* CL_PAR) //definition declare func(X,...y)
    ;

jsCode:
    '{' jsCode '}'
    | .*?;


//tree with alias name in second node
alias:
    AS
    (
        WORD
        |identifiersNames
        | STRING
    )
    (OP_Q CL_Q)?
    ;

//Capture variable name (also accept aliased names).
variable:
    (
        (
        STRING
        |((WORD | NUM)+ (PLUS | MINUS | AT | HASH | DOL | PERCENT | POWER | AMP | STAR | QUESTION_MARK | EXCLAMATION)*)+
        |identifiersNames
        ) 
        (OP_Q CL_Q)? 
        DOT?
    )*
    (
    STRING
    |((WORD | NUM)+ (PLUS | MINUS | AT | HASH | DOL | PERCENT | POWER | AMP | STAR | QUESTION_MARK | EXCLAMATION)*)+
    |identifiersNames
    ) 
    | DOL (WORD(NUM)?)+
    | SORTING
        ;

//Function definition. It accept function with aliases, params or param-free.
function:
    (
        variable 
        OP_PAR 
        (
            (
                function
                |variable
                |STRING+
                |NUM
            ) //Parser should throw when comma occures at first place in parenthesis
            (
                COMMA 
                (
                    function
                    |variable
                    |STRING
                    |NUM
                )
            )*
        )? 
        CL_PAR
    );

whereStatement:
    WHERE expr
    ; 

groupByStatement:
    GROUP_BY 
    (
        parameterWithOptionalAlias
    )
    (
        COMMA 
        (
            parameterWithOptionalAlias
        )
    )*
    ;

orderByStatement:
    ORDER_BY 
        (
            parameterWithOptionalAlias orderBySorting? SORTING?
        )
        (
            COMMA 
            (
                parameterWithOptionalAlias orderBySorting? SORTING?
            )
        )* 
    ;

//Order sorting option keyword.
orderBySorting:
    AS 
    (
        STRING_W 
        |ALPHANUMERIC
        |LONG 
        |DOUBLE
    );

expr:
    specialFunctions
    | inFuction
    | betweenFunction
    | OP_PAR expr CL_PAR
    | expr EQUAL expr
    | expr MATH expr
    | expr AND NOT? expr
    | expr OR NOT? expr
    | ID OP_PAR CL_PAR
    | STRING
    | NUM
    | (TRUE | FALSE)
    | function
    | variable
    ;
    
inFuction:
 (variable | ID OP_PAR CL_PAR) ALL? IN OP_PAR (STRING | NUM | variable) (COMMA (STRING | NUM | variable))* CL_PAR;

betweenFunction:
    (variable | ID OP_PAR CL_PAR) 
    BETWEEN 
        parameter AND parameter;
    
//Functions like morelikethis() or intersect()
specialFunctions:
    specialFunctionName
    OP_PAR
    (
        specialParam alias?
        (
            COMMA specialParam alias?
        )* 
    )?
    CL_PAR
;

specialFunctionName:
    ID
        |FUZZY
        |SEARCH
        |FACET
        |BOOST
        |STARTS_WITH
        |ENDS_WITH
        |MORELIKETHIS
        |INTERSECT
        |EXACT;

specialParam:
     OP_PAR specialParam CL_PAR
    |specialParam EQUAL specialParam
    | variable BETWEEN specialParam
    | specialParam AND specialParam
    | specialParam OR specialParam
    | specialParam MATH specialParam
    | inFuction
    | betweenFunction
    | specialFunctions
    | date
    | function
    | variable
    | identifiersNames
    | STRING
    | NUM;

parameterWithOptionalAlias:
      (
        variable
        |function
      ) 
    alias?;

parameter:
        DOL? (
             function
             |variable
        ) 
;
collectionName:
    WORD
    |STRING
    |identifiersNames;

includeStatement:
    INCLUDE parameter (COMMA parameter)*;

fromAlias:
    AS? (WORD|STRING);

limitStatement:
    LIMIT variable (COMMA variable)?;
    
identifiersNames:
    ALL
    |ALPHANUMERIC
    |AND
    |AS
    |BETWEEN
    |DECLARE
    |DISTINCT
    |DOUBLE
    |ENDS_WITH
    |STARTS_WITH
    |FALSE
    |FACET
    |FROM
    |IN
    |ID
    |INCLUDE
    |INDEX
    |INTERSECT
    |LOAD
    |LONG
    |MATCH
    |MORELIKETHIS
    |NOT
    |NULL
    |OR
    |ORDER_BY
    |SELECT
    |SORTING
    |STRING_W
    |TRUE
    |WHERE
    |WITH
    |EXACT
    |BOOST
    |SEARCH
    |LIMIT
    |FUZZY
    |METADATA
    |TO;
date:
    OP_Q 
    (NULL | dateString)
    TO
    (NULL | dateString) 
    CL_Q
    ;
    
dateString: NUM+ MINUS NUM+ MINUS (NUM | WORD)+ MINUS NUM+ MINUS NUM;
