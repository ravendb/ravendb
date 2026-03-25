import { BaseAutocompleteProvider } from "./baseProvider";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteContext, AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";
import { Scanner } from "../scanner";
import { QuoteUtils } from "../quoteUtils";
import { Token } from "antlr4ts/Token";

type EmbeddingTasksMap = Record<string, Record<string, string[]>>;

export class AutocompleteAiTasks extends BaseAutocompleteProvider implements AutocompleteProvider {

    async collectAsync(ctx: AutocompleteContext): Promise<autoCompleteWordList[]> {
        const { candidates, scanner, writtenText, queryMetaInfo } = ctx;

        if (this.isInsideAiTask(scanner)) {
            const data = await this.fetchAiTasks();
            const taskNames = AutocompleteAiTasks.extractTaskNames(data);
            const quoteType = BaseAutocompleteProvider.detectQuoteType(writtenText) !== "None"
                ? BaseAutocompleteProvider.detectQuoteType(writtenText)
                : "Single";

            return taskNames.map(taskName => ({
                meta: AUTOCOMPLETE_META.function,
                score: AUTOCOMPLETE_SCORING.functionVectorTextualOverload,
                caption: taskName,
                value: QuoteUtils.quote(taskName, quoteType)
            }));
        }

        if (candidates.rules.has(RqlParser.RULE_aiTaskMethod)) {
            const data = await this.fetchAiTasks();
            const taskNames = AutocompleteAiTasks.extractTaskNames(data);
            const completions: autoCompleteWordList[] = taskNames.map(taskName => ({
                meta: AUTOCOMPLETE_META.aiTask,
                score: AUTOCOMPLETE_SCORING.functionVectorTextualOverload,
                caption: `ai.task('${taskName}')`,
                value: `ai.task('${taskName}')`
            }));

            completions.push({
                value: "ai.task(",
                caption: "ai.task(name)",
                meta: AUTOCOMPLETE_META.function,
                score: AUTOCOMPLETE_SCORING.functionVectorTextualOverload,
                snippet: `ai.task('\${1}')`
            });

            return completions;
        }

        if (this.isAtEmbeddingTextOpenParen(scanner)) {
            const collection = queryMetaInfo?.querySourceType === "collection"
                ? queryMetaInfo.querySourceName
                : null;

            if (collection) {
                const data = await this.fetchAiTasks();
                const taskMap = data[collection.toLowerCase()] ?? {};

                return Object.entries(taskMap).flatMap(([taskName, fields]) =>
                    fields.map(fieldName => ({
                        meta: AUTOCOMPLETE_META.function,
                        score: AUTOCOMPLETE_SCORING.functionVectorTextualOverload,
                        caption: `${fieldName}, ai.task('${taskName}')`,
                        value: `${fieldName}, ai.task('${taskName}')`,
                        snippet: `${fieldName}, ai.task('${taskName}')\${0}`
                    }))
                );
            }

            return [];
        }

        if (this.isInsideEmbeddingTextLiteral(scanner)) {
            const data = await this.fetchAiTasks();
            const taskNames = AutocompleteAiTasks.extractTaskNames(data);
            const quoteType = BaseAutocompleteProvider.detectQuoteType(writtenText) !== "None"
                ? BaseAutocompleteProvider.detectQuoteType(writtenText)
                : "Single";

            return taskNames.map(taskName => ({
                meta: AUTOCOMPLETE_META.function,
                score: AUTOCOMPLETE_SCORING.functionVectorTextualOverload,
                caption: `ai.task('${taskName}')`,
                value: `, ai.task(${QuoteUtils.quote(taskName, quoteType)})`,
                snippet: `\${0}, ai.task(${QuoteUtils.quote(taskName, quoteType)})`
            }));
        }

        return [];
    }

    private static extractTaskNames(data: EmbeddingTasksMap): string[] {
        const names = new Set<string>();
        for (const taskMap of Object.values(data)) {
            for (const name of Object.keys(taskMap)) {
                names.add(name);
            }
        }
        return [...names];
    }

    private async fetchAiTasks(): Promise<EmbeddingTasksMap> {
        return new Promise<EmbeddingTasksMap>(resolve =>
            this.metadataProvider.aiTasks(data =>
                resolve(Object.fromEntries(Object.entries(data).map(([k, v]) => [k.toLowerCase(), v])))
            )
        );
    }

    /// ai.task(|
    private isInsideAiTask(scanner: Scanner): boolean {
        scanner.push();
        try {
            if (scanner.tokenType() === RqlParser.OP_PAR && scanner.lookBack() === RqlParser.AI_TASK) {
                return true;
            }

            if (scanner.previous() === false) {
                return false;
            }

            if (scanner.tokenType() !== RqlParser.OP_PAR) {
                return false;
            }
            return scanner.lookBack() === RqlParser.AI_TASK;
        } finally {
            scanner.pop();
        }
    }
    
    private isAtEmbeddingTextOpenParen(scanner: Scanner): boolean {
        scanner.push();
        try {
            if (scanner.tokenType() === RqlParser.OP_PAR
                && scanner.lookBack() === RqlParser.EMBEDDING_TEXT) {
                return true;
            }
            
            const tokenType = scanner.tokenType();
            const isNonContentToken = tokenType === Token.EOF
                || tokenType === RqlParser.CL_PAR
                || tokenType === RqlParser.COMMA;

            if (isNonContentToken && scanner.lookBack() === RqlParser.OP_PAR) {
                scanner.previous();
                return scanner.lookBack() === RqlParser.EMBEDDING_TEXT;
            }
            return false;
        } finally {
            scanner.pop();
        }
    }

    // embedding.text(Name|
    // embedding.text_i8(Name|
    // embedding.text_i1(Name|
    private isInsideEmbeddingTextLiteral(scanner: Scanner): boolean {
        scanner.push();
        try {
            if (scanner.lookBack() !== RqlParser.OP_PAR) {
                return false;
            }
            if (scanner.previous() === false) {
                return false;
            }
            return scanner.lookBack() === RqlParser.EMBEDDING_TEXT;
        } finally {
            scanner.pop();
        }
    }
}
