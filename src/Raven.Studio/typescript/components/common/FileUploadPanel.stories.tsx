import { Meta, StoryObj } from "@storybook/react-webpack5";
import { useState } from "react";
import FileUploadPanel from "components/common/FileUploadPanel";
import { FileUploadItem, FileUploadItemStatus } from "components/common/FileUploadList";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/FileUploadPanel",
    decorators: [withStorybookContexts, withBootstrap5],
    component: FileUploadPanel,
} satisfies Meta<typeof FileUploadPanel>;

function fakeFile(name: string, size: number): File {
    const file = new File(["x"], name);
    Object.defineProperty(file, "size", { value: size });
    return file;
}

const sampleFiles = [
    fakeFile("product-demo-recording.mp4", 4_810_000),
    fakeFile("release-walkthrough.mp4", 4_180_000),
    fakeFile("release-notes.md", 16_180),
    fakeFile("an-intentionally-long-attachment-name-for-truncation.md", 8_900),
    fakeFile("cluster-topology.json", 122_000),
    fakeFile("backup-2026-09-01.zip", 88_400_000),
    fakeFile("résumé-template.pdf", 41_300),
    fakeFile("server-config.yml", 3_200),
];

interface StoryProps {
    fileCount: number;
    collapsedCount: number;
    validExtensions?: string[];
    statuses?: Partial<Record<number, FileUploadItemStatus>>;
    progressAt?: number;
}

function FileUploadPanelStory({ fileCount, collapsedCount, validExtensions, statuses = {}, progressAt }: StoryProps) {
    const [files, setFiles] = useState<File[]>(() => sampleFiles.slice(0, fileCount));

    const items: FileUploadItem[] = files.map((file, index) => {
        const status = statuses[index] ?? "selected";
        return status === "uploading"
            ? { file, status, loaded: Math.floor(file.size * (progressAt ?? 0.45)), total: file.size }
            : { file, status };
    });

    return (
        <div style={{ maxWidth: 720 }}>
            <FileUploadPanel
                items={items}
                collapsedCount={collapsedCount}
                validExtensions={validExtensions}
                onChange={(added) => setFiles((current) => [...current, ...added])}
                onRemove={(file) => setFiles((current) => current.filter((x) => x !== file))}
                onCancel={() => undefined}
                onClearAll={() => setFiles([])}
            />
        </div>
    );
}

const render = (args: StoryProps) => <FileUploadPanelStory key={args.fileCount} {...args} />;

const argTypes = {
    fileCount: { control: { type: "range" as const, min: 0, max: sampleFiles.length, step: 1 } },
    collapsedCount: { control: { type: "range" as const, min: 1, max: sampleFiles.length, step: 1 } },
};

export const Empty: StoryObj<StoryProps> = {
    args: { fileCount: 0, collapsedCount: 3 },
    argTypes,
    render,
};

/** The accepted types show as a badge in the drop area's corner. */
export const WithRestrictedFileTypes: StoryObj<StoryProps> = {
    args: { fileCount: 0, collapsedCount: 3, validExtensions: ["png", "jpg", "pdf"] },
    argTypes,
    render,
};

/** Exactly one over the cap, so the list stays whole rather than hiding a single row. */
export const JustUnderTheCollapseThreshold: StoryObj<StoryProps> = {
    args: { fileCount: 4, collapsedCount: 3 },
    argTypes,
    render,
};

/** Two over the cap: the fourth row peeks out from under the fade that hosts the toggle. */
export const CollapsedWithFade: StoryObj<StoryProps> = {
    args: { fileCount: 8, collapsedCount: 3 },
    argTypes,
    render,
};

export const Uploading: StoryObj<StoryProps> = {
    args: {
        fileCount: 5,
        collapsedCount: 3,
        statuses: { 0: "uploaded", 1: "uploading" },
        progressAt: 0.45,
    },
    argTypes,
    render,
};

export const Finished: StoryObj<StoryProps> = {
    args: {
        fileCount: 4,
        collapsedCount: 3,
        statuses: { 0: "uploaded", 1: "uploaded", 2: "skipped", 3: "failed" },
    },
    argTypes,
    render,
};
