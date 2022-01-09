import { AbstractParseTreeVisitor } from "antlr4ts/tree";
import { BaseRqlParserVisitor } from "./generated/BaseRqlParserVisitor";
import {
    CollectionByIndexContext,
    CollectionByNameContext, JsFunctionContext,
} from "./generated/BaseRqlParser";
import { QuerySource } from "./providers/baseProvider";
import { QuoteUtils } from "./quoteUtils";

export interface JsFunctionDeclaration {
    name: string;
}

export class RqlQueryMetaInfo {
    fromAlias: string; 
    queryType: rqlQueryType;
    querySourceType: QuerySource;
    querySourceName: string;
    jsFunctions: JsFunctionDeclaration[];
}

export class RqlQueryVisitor extends AbstractParseTreeVisitor<RqlQueryMetaInfo> implements BaseRqlParserVisitor<RqlQueryMetaInfo> {
    
    private readonly meta: RqlQueryMetaInfo;
    
    constructor(queryType: rqlQueryType) {
        super();
        this.meta = new RqlQueryMetaInfo();
        this.meta.queryType = queryType;
        this.meta.querySourceType = "unknown";
        this.meta.jsFunctions = [];
    }

    visitCollectionByIndex(ctx: CollectionByIndexContext): RqlQueryMetaInfo {
        const alias = ctx.aliasWithOptionalAs()?.aliasName()?.text;
        if (alias) {
            this.meta.fromAlias = alias;
        }
        this.meta.querySourceType = "index";
        this.meta.querySourceName = QuoteUtils.unquote(ctx.indexName().text);
        return this.visitChildren(ctx);
    }
    
    visitCollectionByName(ctx: CollectionByNameContext): RqlQueryMetaInfo {
        const alias = ctx.aliasWithOptionalAs()?.aliasName()?.text;
        if (alias) {
            this.meta.fromAlias = alias;
        }
        this.meta.querySourceType = "collection";
        this.meta.querySourceName = QuoteUtils.unquote(ctx.collectionName().text);
        return this.visitChildren(ctx);
    }

    visitJsFunction(ctx: JsFunctionContext): RqlQueryMetaInfo {
        const functionName = ctx.JFN_WORD(0);
        this.meta.jsFunctions.push({
            name: functionName.text
        });

        return this.visitChildren(ctx);
    }
    
    protected defaultResult(): RqlQueryMetaInfo {
        return this.meta;
    }


}
