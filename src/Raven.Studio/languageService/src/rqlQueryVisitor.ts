import { AbstractParseTreeVisitor } from "antlr4ts/tree";
import { ScopedSymbol, SymbolTable } from "antlr4-c3";
import { RqlParserVisitor } from "./generated/RqlParserVisitor";

export class RqlQueryVisitor extends AbstractParseTreeVisitor<SymbolTable> implements RqlParserVisitor<SymbolTable> {
    constructor(
        protected readonly symbolTable = new SymbolTable("", {}),
        protected scope = symbolTable.addNewSymbolOfType(ScopedSymbol, undefined)) {
        super();
    }

    protected defaultResult(): SymbolTable {
        return this.symbolTable;
    }
}
