import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import queryCriteria from "models/database/query/queryCriteria";

interface Argument {
    key: string;
    value: any;
}

interface UseToolQueryDetailsOptions {
    queryText: string;
    parametersFromUser: Record<string, string>;
    parametersFromModel: string;
}

export default function useToolQueryDetails({
    queryText,
    parametersFromUser,
    parametersFromModel,
}: UseToolQueryDetailsOptions) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const getLlmParametersForQuery = (matches: string[]): Argument[] => {
        try {
            const parametersObject = JSON.parse(parametersFromModel);
            return matches
                .map((x) => ({ key: x, value: parametersObject[x] }))
                .filter((x) => x.value && !Object.keys(parametersFromUser).includes(x.key));
        } catch {
            return [];
        }
    };

    const getAgentParametersForQuery = (matches: string[]): Argument[] => {
        return matches.map((x) => ({ key: x, value: parametersFromUser?.[x] })).filter((x) => x.value);
    };

    const getArgumentFormattedValue = (
        value: string | Raven.Client.Documents.AI.AiConversationParameter
    ): string | number => {
        const extractedValue = typeof value === "object" && "Value" in value ? value.Value : value;

        if (typeof extractedValue === "number") {
            return extractedValue;
        }
        return JSON.stringify(extractedValue);
    };

    const getQueryWithParameters = (): string => {
        const regexToFind$: RegExp = /\$\w+/g;
        const allMatches = queryText.match(regexToFind$)?.map((x) => x.replace("$", "")) || [];
        const uniqueMatches = [...new Set(allMatches)];

        const llmParametersForQuery = getLlmParametersForQuery(uniqueMatches);
        const agentParametersForQuery = getAgentParametersForQuery(uniqueMatches);

        let resultQuery = "";

        if (llmParametersForQuery.length > 0) {
            resultQuery += `// LLM parameters\n`;
            resultQuery += llmParametersForQuery
                .map((x) => `$${x.key} = ${getArgumentFormattedValue(x.value)}`)
                .join("\n");
            resultQuery += "\n\n";
        }

        if (agentParametersForQuery.length > 0) {
            resultQuery += `// Agent parameters\n`;
            resultQuery += agentParametersForQuery
                .map((x) => `$${x.key} = ${getArgumentFormattedValue(x.value)}`)
                .join("\n");
            resultQuery += "\n\n";
        }

        resultQuery += queryText;

        return resultQuery;
    };

    const queryWithParameters = getQueryWithParameters();

    const linkToQuery = () => {
        const query = queryCriteria.empty();

        query.queryText(queryWithParameters);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();
        savedQueriesStorage.saveAndNavigate(databaseName, queryDto, {
            newWindow: true,
        });
    };

    return {
        linkToQuery,
        queryWithParameters,
    };
}
