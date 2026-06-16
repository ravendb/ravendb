import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DebugPackage from "./DebugPackage";
import DebugPackageAnalysisView from "./partials/DebugPackageAnalysisView";
import { AboutViewHeading } from "components/common/AboutView";
import { DebugPackageStubs } from "test/stubs/DebugPackageStubs";

export default {
    title: "Pages/Manage Server/Debug Package Analyzer",
    component: DebugPackage,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/0AC6Rm0JBS5FBt3rsRKxOl/Pages---Debug-Package-Analyzer?node-id=576-4496",
        },
    },
} satisfies Meta<typeof DebugPackage>;

export const EmptyState: StoryObj<typeof DebugPackage> = {
    name: "Empty (upload) state",
    render: () => <DebugPackage />,
};

export const ClusterContext: StoryObj<typeof DebugPackage> = {
    name: "Loaded - Cluster context",
    render: () => (
        <div className="flex-window padding-xs">
            <div className="bs5 debug-package-analyzer content-margin">
                <AboutViewHeading
                    title="Debug Package Analyzer"
                    backUrl="#admin/settings/debugPackage"
                    marginBottom={1}
                />
                <p className="text-muted fs-5 mb-4">Examine the package to identify the problem with your server</p>
                <DebugPackageAnalysisView
                    summary={DebugPackageStubs.analysisSummary()}
                    fileName="2025-11-19 10-55-11 Cluster Wide.zip"
                    onReset={() => undefined}
                />
            </div>
        </div>
    ),
};
