import { Meta, StoryObj } from "@storybook/react";
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
        const getPaths = args.isErrored
            ? async () => {
                  throw new Error();
              }
            : async () => args.paths;

        return (
            <PathSelector
                getPaths={getPaths}
                getPathDependencies={(x) => [x]}
                handleSelect={() => null}
                defaultPath={args.defaultPath}
                selectorButtonName={args.selectorButtonName}
                selectorTitle={args.selectorTitle}
                placeholder={args.placeholder}
                disabled={args.disabled}
            />
        );
    },
    args: {
        paths: ["C:\\Desktop", "C:\\temp"],
        defaultPath: "C:\\",
        selectorButtonName: "Select path",
        selectorTitle: "Select path",
        placeholder: "Enter path",
        disabled: false,
        isErrored: false,
    },
};
