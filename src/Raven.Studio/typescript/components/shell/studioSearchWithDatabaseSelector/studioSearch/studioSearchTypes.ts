import { RangeTuple } from "fuse.js";
import IconName from "typings/server/icons";

export type StudioSearchMenuItemType =
    | "serverMenuItem"
    | "documentsMenuItem"
    | "indexesMenuItem"
    | "tasksMenuItem"
    | "settingsMenuItem"
    | "statsMenuItem";

export type StudioSearchItemType = StudioSearchMenuItemType | "collection" | "document" | "task" | "index" | "database";

export type StudioSearchItemEvent = React.MouseEvent<HTMLElement, MouseEvent> | KeyboardEvent;

export interface StudioSearchResult {
    server: StudioSearchResultItem[];
    database: {
        collections: StudioSearchResultItem[];
        documents: StudioSearchResultItem[];
        indexes: StudioSearchResultItem[];
        tasks: StudioSearchResultItem[];
        settings: StudioSearchResultItem[];
        stats: StudioSearchResultItem[];
    };
    switchToDatabase: StudioSearchResultItem[];
}

export type StudioSearchResultDatabaseGroup = keyof StudioSearchResult["database"];

export interface SearchInnerAction {
    text: string;
    alternativeTexts?: string[];
}

export interface StudioSearchItem {
    type: StudioSearchItemType;
    text: string;
    onSelected: (e: StudioSearchItemEvent) => void;
    alternativeTexts?: string[];
    route?: string;
    icon?: IconName;
    innerActions?: SearchInnerAction[];
}

export interface StudioSearchResultItem {
    id: string;
    type: StudioSearchItemType;
    text: string;
    onSelected: (e: StudioSearchItemEvent) => void;
    icon?: IconName;
    route?: string;
    indices?: readonly RangeTuple[];
    innerActionText?: string;
    innerActionIndices?: readonly RangeTuple[];
}

export type OngoingTaskWithBroker = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask & {
    BrokerType?: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType;
};
