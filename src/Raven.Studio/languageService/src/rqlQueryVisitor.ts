import { AbstractParseTreeVisitor } from "antlr4ts/tree";
import { ScopedSymbol, SymbolTable } from "antlr4-c3";
import { BaseRqlParserVisitor } from "./generated/BaseRqlParserVisitor";

export class RqlQueryVisitor extends AbstractParseTreeVisitor<SymbolTable> implements BaseRqlParserVisitor<SymbolTable> {
    constructor(
        protected readonly symbolTable = new SymbolTable("", {}),
        protected scope = symbolTable.addNewSymbolOfType(ScopedSymbol, undefined)) {
        super();
    }

    protected defaultResult(): SymbolTable {
        return this.symbolTable;
    }
}
