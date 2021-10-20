import { AbstractParseTreeVisitor } from "antlr4ts/tree";
import { BaseRqlParserVisitor } from "./generated/BaseRqlParserVisitor";
import {
    CollectionByIndexContext,
    CollectionByNameContext,
} from "./generated/BaseRqlParser";
import { QuerySource } from "./providers/baseProvider";
import { AutocompleteUtils } from "./autocompleteUtils";

export class RqlQueryMetaInfo {
    fromAlias: string; 
    queryType: rqlQueryType;
    querySourceType: QuerySource;
    querySourceName: string;
}

export class RqlQueryVisitor extends AbstractParseTreeVisitor<RqlQueryMetaInfo> implements BaseRqlParserVisitor<RqlQueryMetaInfo> {
    
    private readonly meta: RqlQueryMetaInfo;
    
    constructor(queryType: rqlQueryType) {
        super();
        this.meta = new RqlQueryMetaInfo();
        this.meta.queryType = queryType;
        this.meta.querySourceType = "unknown";
    }

    visitCollectionByIndex(ctx: CollectionByIndexContext): RqlQueryMetaInfo {
        const alias = ctx.aliasWithOptionalAs()?.aliasName()?.text;
        if (alias) {
            this.meta.fromAlias = alias;
        }
        this.meta.querySourceType = "index";
        this.meta.querySourceName = AutocompleteUtils.unquote(ctx.indexName().text);
        return this.visitChildren(ctx);
    }

    visitCollectionByName(ctx: CollectionByNameContext): RqlQueryMetaInfo {
        const alias = ctx.aliasWithOptionalAs()?.aliasName()?.text;
        if (alias) {
            this.meta.fromAlias = alias;
        }
        this.meta.querySourceType = "collection";
        this.meta.querySourceName = AutocompleteUtils.unquote(ctx.collectionName().text);
        return this.visitChildren(ctx);
    }
    
    protected defaultResult(): RqlQueryMetaInfo {
        return this.meta;
    }


}
