import { CodeCompletionCore, SymbolTable } from "antlr4-c3";
import { RqlParser } from "./generated/RqlParser";
import { ParseTree } from "antlr4ts/tree/ParseTree";
import { TokenPosition } from "./types";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";




/*
console.log("RULES = ", Array.from(candidates.rules.keys()).map(x => RqlParser.ruleNames[x]));

if (candidates.rules.has(RqlParser.RULE_fromStatement)) {
    const collections = ["orders", "products", "employees"];

    const collectionsAutocomplete = filterTokens(textToMatch, collections).map(x => ({
        caption: x,
        score: 200,
        meta: "collection",
        value: x
    }))
    completions.push(...collectionsAutocomplete);
}

if (candidates.rules.has(RqlParser.RULE_indexName)) {
    const indexes = ["Products/ByName", "Orders/ByTest"];

    completions.push(...indexes.map(x => ({
        caption: x,
        score: 200,
        value: x,
        meta: "index name"
    })))
}*/
