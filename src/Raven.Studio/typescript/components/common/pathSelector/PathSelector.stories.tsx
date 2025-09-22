import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import PathSelector, { PathSelectorProps } from "./PathSelector";
import React from "react";

export default {
    title: "Bits/PathSelector",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface PathSelectorStoryArgs extends Omit<PathSelectorProps, "getPaths"> {
    paths: string[];
    isErrored: boolean;
}

export const PathSelectorStory: StoryObj<PathSelectorStoryArgs> = {
    name: "Path Selector",
    render: (args) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const getPaths = (_: string) => {
            return async () => {
                if (args.isErrored) {
                    throw new Error();
                }

                return args.paths;
            };
        };

        return (
            <PathSelector
                getPathsProvider={(path: string) => getPaths(path)}
                getPathDependencies={(path: string) => [path]}
                handleSelect={() => null}
                defaultPath={args.defaultPath}
                selectorTitle={args.selectorTitle}
                placeholder={args.placeholder}
                disabled={args.disabled}
            />
        );
    },
    args: {
        paths: ["C:\\Desktop", "C:\\temp"],
        defaultPath: "C:\\",
        selectorTitle: "Select path",
        placeholder: "Enter path",
        disabled: false,
        isErrored: false,
    },
};
