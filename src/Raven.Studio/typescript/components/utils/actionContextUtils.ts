import { DatabaseActionContexts } from "components/common/MultipleDatabaseLocationSelector";

export default class ActionContextUtils {
    static getContexts(locations: databaseLocationSpecifier[], orchestratorTags?: string[]): DatabaseActionContexts[] {
        return [...new Set(locations.map((x) => x.nodeTag))].map((nodeTag) => ({
            nodeTag,
            shardNumbers: locations
                .filter((x) => x.nodeTag === nodeTag && x.shardNumber > -1)
                .map((x) => x.shardNumber),
            includeOrchestrator: !!orchestratorTags?.includes(nodeTag),
        }));
    }

    static getLocations(nodeTag: string, shardNumbers: number[]): databaseLocationSpecifier[] {
        if (shardNumbers.length === 0) {
            return [{ nodeTag }];
        }

        return shardNumbers.map((shardNumber) => ({
            nodeTag,
            shardNumber,
        }));
    }

    static isSharded(contexts: DatabaseActionContexts[]): boolean {
        return contexts.some((x) => x.shardNumbers?.length > 0);
    }

    static showContextSelector(contexts: DatabaseActionContexts[]): boolean {
        return contexts.length > 1 || ActionContextUtils.isSharded(contexts);
    }
}
