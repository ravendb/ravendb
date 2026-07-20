import { MouseEvent, MouseEventHandler } from "react";
import { SelectOption } from "components/common/select/Select";
import { loadableData } from "components/models/common";
import { StoryFn } from "@storybook/react-webpack5";
import typeUtils = require("common/typeUtils");

export function withPreventDefault(action: (...args: any[]) => void): MouseEventHandler<HTMLElement> {
    return (e: MouseEvent<HTMLElement>) => {
        e.preventDefault();
        action();
    };
}

export function withNestedSubmit(action: (...args: any[]) => void) {
    return (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        e.stopPropagation();
        action();
    };
}

export function createIdleState<T>(initialData?: T): loadableData<T> {
    return {
        status: "idle",
        data: initialData,
    };
}

export function createSuccessState<T>(data: T): loadableData<T> {
    return {
        status: "success",
        data,
    };
}

export function createLoadingState(): loadableData<undefined> {
    return {
        status: "loading",
    };
}

export function createFailureState(error?: any): loadableData<undefined> {
    return {
        status: "failure",
        error,
    };
}

export async function delay(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export function databaseLocationComparator(lhs: databaseLocationSpecifier, rhs: databaseLocationSpecifier) {
    return lhs.nodeTag === rhs.nodeTag && (lhs.shardNumber ?? null) === (rhs.shardNumber ?? null);
}

export function boundCopy<TArgs>(story: StoryFn<TArgs>, args?: TArgs): StoryFn<TArgs> {
    const copy = story.bind({});
    copy.args = args;
    return copy;
}

export async function tryHandleSubmit<T>(promise: () => Promise<T>) {
    try {
        return await promise();
    } catch (e) {
        console.error(e);
    }
}

export type TryCatchResult<T> =
    | {
          status: "success";
          data: T;
      }
    | {
          status: "error";
          error: string;
      };

export async function tryCatch<T>(promiseFn: () => Promise<T>): Promise<TryCatchResult<T>> {
    try {
        const response = await promiseFn();
        return { status: "success", data: response };
    } catch (error) {
        return {
            status: "error",
            error: error instanceof Error ? error.message : "Unknown error",
        };
    }
}

// source: https://stackoverflow.com/a/55266531
type AtLeastOne<T> = [T, ...T[]];

export const exhaustiveStringTuple =
    <T extends string>() =>
    <L extends AtLeastOne<T>>(
        ...x: L extends any ? (Exclude<T, L[number]> extends never ? L : Exclude<T, L[number]>[]) : never
    ) =>
        x;
// ---

export const milliSecondsInWeek = 1000 * 3600 * 24 * 7;

export const availableGlacierRegions: SelectOption<string>[] = [
    { label: "Africa (Cape Town) - af-south-1", value: "af-south-1" },
    { label: "Asia Pacific (Hong Kong) - ap-east-1", value: "ap-east-1" },
    { label: "Asia Pacific (Jakarta) - ap-southeast-3", value: "ap-southeast-3" },
    { label: "Asia Pacific (Mumbai) - ap-south-1", value: "ap-south-1" },
    { label: "Asia Pacific (Osaka) - ap-northeast-3", value: "ap-northeast-3" },
    { label: "Asia Pacific (Seoul) - ap-northeast-2", value: "ap-northeast-2" },
    { label: "Asia Pacific (Singapore) - ap-southeast-1", value: "ap-southeast-1" },
    { label: "Asia Pacific (Sydney) - ap-southeast-2", value: "ap-southeast-2" },
    { label: "Asia Pacific (Tokyo) - ap-northeast-1", value: "ap-northeast-1" },
    { label: "AWS GovCloud (US-East) - us-gov-east-1", value: "us-gov-east-1" },
    { label: "AWS GovCloud (US-West) - gov-west-1", value: "us-gov-west-1" },
    { label: "Canada (Central) - ca-central-1", value: "ca-central-1" },
    { label: "China (Beijing) - cn-north-1", value: "cn-north-1" },
    { label: "China (Ningxia) - cn-northwest-1", value: "cn-northwest-1" },
    { label: "Europe (Frankfurt) - eu-central-1", value: "eu-central-1" },
    { label: "Europe (Ireland) - eu-west-1", value: "eu-west-1" },
    { label: "Europe (London) - eu-west-2", value: "eu-west-2" },
    { label: "Europe (Milan) - eu-south-1", value: "eu-south-1" },
    { label: "Europe (Paris) - eu-west-3", value: "eu-west-3" },
    { label: "Europe (Stockholm) - eu-north-1", value: "eu-north-1" },
    { label: "Israel (Tel Aviv) - il-central-1", value: "il-central-1" },
    { label: "Middle East (Bahrain) - me-south-1", value: "me-south-1" },
    { label: "South America (São Paulo) - sa-east-1", value: "sa-east-1" },
    { label: "US East (N. Virginia) - us-east-1", value: "us-east-1" },
    { label: "US East (Ohio) - us-east-2", value: "us-east-2" },
    { label: "US West (N. California) - us-west-1", value: "us-west-1" },
    { label: "US West (Oregon) - us-west-2", value: "us-west-2" },
];

export const availableS3Regions: SelectOption<string>[] = typeUtils.sortBy(
    [
        ...availableGlacierRegions,
        { label: "Asia Pacific (Hyderabad) - ap-south-2", value: "ap-south-2" },
        { label: "Asia Pacific (Melbourne) - ap-southeast-4", value: "ap-southeast-4" },
        { label: "Europe (Spain) - eu-south-2", value: "eu-south-2" },
        { label: "Europe (Zurich) - eu-central-2", value: "eu-central-2" },
        { label: "Middle East (UAE) - me-central-1", value: "me-central-1" },
    ],
    (region) => region.label.toLowerCase()
);

export type OmitIndexSignature<T> = {
    [K in keyof T as string extends K ? never : K]: T[K];
};

export const storageClassOptions: SelectOption<Raven.Client.Documents.Operations.Backups.S3StorageClass>[] = [
    { value: "Standard", label: "Standard" },
    { value: "IntelligentTiering", label: "Intelligent Tiering" },
    { value: "StandardInfrequentAccess", label: "Standard Infrequent Access" },
    { value: "OneZoneInfrequentAccess", label: "One Zone Infrequent Access" },
    { value: "GlacierInstantRetrieval", label: "Glacier Instant Retrieval" },
    { value: "Glacier", label: "Glacier Flexible Retrieval" },
    { value: "ReducedRedundancy", label: "Reduced Redundancy" },
    { value: "DeepArchive", label: "Deep Archive" },
    { value: "ExpressOneZone", label: "Express One Zone" },
];

export type StringWithAutocomplete<T> = T | (string & NonNullable<unknown>);

export const allLogLevels = exhaustiveStringTuple<Sparrow.Logging.LogLevel>()(
    "Trace",
    "Debug",
    "Info",
    "Warn",
    "Error",
    "Fatal",
    "Off"
);

export const logLevelRelevances: Record<Sparrow.Logging.LogLevel, number> = {
    Trace: 0,
    Debug: 1,
    Info: 2,
    Warn: 3,
    Error: 4,
    Fatal: 5,
    Off: 6,
};

export const allLogFilterActions = exhaustiveStringTuple<Sparrow.Logging.LogFilterAction>()(
    "Ignore",
    "IgnoreFinal",
    "Log",
    "LogFinal",
    "Neutral"
);
export const logLevelOptions: SelectOption<Sparrow.Logging.LogLevel>[] = allLogLevels.map((level) => ({
    label: level,
    value: level,
}));

export const logFilterActionOptions: SelectOption<Sparrow.Logging.LogFilterAction>[] = allLogFilterActions.map(
    (action) => ({
        label: action,
        value: action,
    })
);

export const allAiExternalProviders = ["Azure OpenAI", "Google AI", "Ollama", "OpenAI", "Mistral AI"];

export type RecursiveRequired<T> = Required<{
    [P in keyof T]: T[P] extends object | undefined ? RecursiveRequired<Required<T[P]>> : T[P];
}>;
