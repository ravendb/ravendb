parser grammar RqlParser;
options { tokenVocab = RqlLexer; }

@parser::members
{
    public from: boolean = false;
    public where: boolean = false;
    public groupBy: boolean = false;
    public orderBy: boolean = false;
    public select: boolean = false;
    
    public afterFrom()
    {
       this.from = false;
    }
      
    public afterGroupBy()
    {
       this.afterFrom();
       this.groupBy = false;
    }
    
    public afterWhere()
    {
       this.where = false;
       this.afterGroupBy();
    }
             
    public afterLoad()
    {
       this.afterWhere();
    }
    
    public afterOrderBy()
    {
       this.afterLoad();
       this.orderBy = false;
    }
    
    public afterSelect()
    {
       this.select = false;
       this.afterOrderBy();
    }
    
    public afterInclude()
    {
       this.afterSelect();
    }
    
    public afterLimit()
    {
       this.afterInclude();
    }
}

prog:
    (jsFunction)* 
    fromStatement 
    groupByStatement?
    whereStatement? 
    loadStatement? 
    orderByStatement? 
    selectStatement? 
    includeStatement?
    limitStatement?
    EOF
    ;
    
//          FROM STATEMENT          //
fromMode:
    FROM
    ;

fromStatement:
    fromMode INDEX collection=collectionName fromAlias? #CollectionByIndex
    |FROM ALL_DOCS #AllCollections
    |fromMode collection=collectionName fromAlias? #CollectionByName 
    ;
    
collectionName:
    WORD
    |STRING
    |identifiersAllNames
    ;

//We can use aliases like:
//from X as t
//from X t
fromAlias:
    AS? 
    (
    WORD
    |STRING
    |identifiersAllNames
    )
    ;
    
//          GROUP BY STATEMENT          //    
groupByMode:
    GROUP_BY
    {
        this.afterFrom();
        this.groupBy = true;
        
    }
    ;
    
groupByStatement:
     groupByMode
    (
        value = parameterWithOptionalAlias
    )
    (
        COMMA 
        (
            parameterWithOptionalAlias
        )
    )*
    ;
    
    
//          WHERE STATEMENT         //
whereMode:
    WHERE
    {
        this.afterGroupBy();
        this.where = true;
    };
    
whereStatement:
    whereMode 
    expr
    ;
    
expr:
    left=expr binary right=expr #binaryExpression
    | OP_PAR expr CL_PAR #opPar
    |left=exprValue EQUAL right=exprValue #equalExpression
    |left=exprValue MATH right=exprValue #mathExpression
    |specialFunctions #specialFunctionst
    |inFunction #inExpr
    |betweenFunction #betweenExpr
    |function #normalFunc
    |TRUE AND NOT? expr #booleanExpression
    ;

binary:
    AND NOT
    |OR NOT
    |AND
    |OR
    ;
   
exprValue:
    literal #parameterExpr
    ;

inFunction:
    value=literal 
    ALL? IN 
    OP_PAR
        first=literal 
        (
            COMMA 
            next=literal
        )* 
    CL_PAR
    ;

betweenFunction:
    value=literal
    BETWEEN 
        from=literal 
    AND 
        to=literal
    ;
    
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
    |EXACT
    ;

specialParam:
     OP_PAR specialParam CL_PAR
    |specialParam EQUAL specialParam
    |variable BETWEEN specialParam
    |specialParam AND specialParam
    |specialParam OR specialParam
    |specialParam MATH specialParam
    |inFunction
    |betweenFunction
    |specialFunctions
    |date
    |function
    |variable
    |identifiersAllNames
    |NUM
;

//          LOAD STATEMENT          //
//Accept:n
// load x as X
//also list of load eg (load x as y, y as p) etc 
loadMode:
    LOAD
    {
       this.afterWhere();
    }
    ;
    
loadStatement:
    loadMode 
    item=loadDocumentByName
    (COMMA loadDocumentByName)*
    ;

loadDocumentByName:
    name=variable 
    as=alias
    ;

//          ORDER BY            //
orderByMode:
    ORDER_BY
    {
        this.afterGroupBy();
        this.orderBy = true;
    };

orderByStatement:
    orderByMode 
    value=orderByItem
        (
            COMMA 
            (
                orderByItem
            )
        )* 
        
    ;

orderByItem:
    value=literal
    order=orderBySorting?
    orderValueType=orderByOrder?
    ;

//Order sorting option keyword.
orderBySorting:
    AS 
    sortingMode=orderBySortingAs 
    
    ;

orderBySortingAs:
    STRING_W 
    |ALPHANUMERIC
    |LONG 
    |DOUBLE
    ;
orderByOrder:
    SORTING
    ;

//          SELECT STATEMENT            //
selectMode:
    SELECT
    {
        this.afterOrderBy();
        this.select = true;
    }
    ;
    
selectStatement:
    //  Select only individual fields e.g. "select Column1 (as x)?, Column2"
     selectMode DISTINCT STAR  limitStatement? #getAllDistinct
    |selectMode DISTINCT?
        field=projectField
    (
        COMMA projectField
    )*  limitStatement?
    #ProjectIndividualFields
    // Please notice that JavaScript segment is on 2 channel so we don't get any information on first on about JS
    // code so we accept "SELECT LIMIT $p1, $p2". To make sure it's correct we need check channel(2).IsEmpty or something like this.
    |selectMode limitStatement? #javascriptCode
    ;
    
projectField:
    (
        literal
        |specialFunctions
    )
    alias?
    ;

//JS header
//Accept declare function(params...). We don't get any JS CODE on first channel so we need to check it like in example from SELECT statement
jsFunction: 
    JS_FUNCTION_DECLARATION variable 
        OP_PAR 
            WORD 
                (
                    COMMA 
                    variable
                )* 
        CL_PAR
    //definition declare func(X,...y)
    ;

alias:
    AS
    name=aliasName
    asArray?
    ;
    
aliasName:
    (
        WORD
        |identifiersAllNames
        |STRING
    )
    ;
    
// @metadata.
// array like Array[].Empty[]. etc
prealias:
    METADATA DOT
    |(WORD asArray? DOT)+
    ;
asArray:
    OP_Q CL_Q
    ;


//          INCLUDE STATEMENT           //
includeMode:
    INCLUDE
    {
        this.afterSelect();
    }
    ;
    
includeStatement:
    includeMode 
    literal 
    (
        COMMA 
        literal
    )*
        ;


//          LIMIT STATEMENT          //
limitStatement:
        LIMIT     
        variable 
        (
            (
             COMMA
             |OFFSET
             ) 
            variable
        )?
        ;


//          UTILS SEGMENT           //
    
//Capture variable name (also accept aliased names).
variable:
    prealias*
    (
      cacheParam
     |param
    )
    ; 
    
param:
    (
    NUM
    |WORD
    |date
    |STRING
    |ID OP_PAR CL_PAR
    |identifiersAllNames
    ) 
    asArray?
    ;

literal:
    DOL? 
    (
         function
         |variable
    ) 
    ;
cacheParam:
    DOL WORD
    ;


parameterWithOptionalAlias:
    value=variableOrFunction
    as=alias?
    ;

variableOrFunction:
    variable
    |function
    ;

//Function definition. It accept function with aliases, params or param-free.
function:
    (
        variable 
        OP_PAR 
        (
            (
                literal
            ) //Parser should throw when comma occures at first place in parenthesis
            (
                COMMA 
                (
                    literal
                )
            )*
        )? 
        CL_PAR
    )
    ;

//Use tokens like string
   
identifiersAllNames:
    ALL
    |AND
    |AS
    |BETWEEN
    |DECLARE
    |DISTINCT
    |ENDS_WITH
    |STARTS_WITH
    |FALSE
    |FACET
    |FROM
    |IN
    |ID
    |INCLUDE
    |INTERSECT
    |LOAD
    |LONG
    |MATCH
    |MORELIKETHIS
    |NULL
    |OR
    |ORDER_BY
    |STRING_W
    |TRUE
    |WHERE
    |WITH
    |EXACT
    |BOOST
    |SEARCH
    |FUZZY
    |METADATA
    |TO
    |{!this.select}? LIMIT
    |{!this.groupBy}? GROUP_BY
    |{!this.where}? NOT
    |{!this.orderBy}? SORTING
    |{!this.orderBy}? ALPHANUMERIC
    |{!this.orderBy}? DOUBLE
    ;
    
//Accept date range [DATE TO DATE]
date:
    OP_Q 
    (
        NULL
        |dateString
    )
    TO
    (
        NULL
        |dateString
    )
    CL_Q
    ;
    
dateString: 
    WORD DOT NUM;
