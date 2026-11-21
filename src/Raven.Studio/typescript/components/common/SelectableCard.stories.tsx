import { useState } from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import SelectableCard from "./SelectableCard";

type CardId = "backup" | "snapshot";

interface SelectableCardStoryArgs {
    selectedCard: CardId | "none";
    isDisabled: boolean;
    hasBadge: boolean;
    hasPopover: boolean;
    hasLongValues: boolean;
    containerWidth: number;
}

const LONG_TITLE = "Snapshot of the whole database with all of its indexes, counters, time series and attachments";
const LONG_DESCRIPTION =
    "Instant restoration, but it takes longer to create and uses more storage. ".repeat(4) +
    "production-europe-orders-nightly-backups-with-a-very-long-name-that-cannot-be-broken";

function SelectableCardStory({
    selectedCard,
    isDisabled,
    hasBadge,
    hasPopover,
    hasLongValues,
    containerWidth,
}: SelectableCardStoryArgs) {
    const [selected, setSelected] = useState<CardId | "none">(selectedCard);

    return (
        <div role="radiogroup" aria-label="Backup type" className="vstack gap-2" style={{ maxWidth: containerWidth }}>
            <SelectableCard
                icon="backup"
                title="Backup"
                description="Faster to create and smaller, but requires index rebuilding on restore."
                isSelected={selected === "backup"}
                onClick={() => setSelected("backup")}
                popoverMessage={hasPopover ? "Good when storage space matters more than a fast restore." : undefined}
            />
            <SelectableCard
                icon="snapshot-backup"
                title={hasLongValues ? LONG_TITLE : "Snapshot"}
                description={
                    hasLongValues
                        ? LONG_DESCRIPTION
                        : "Instant restoration, but takes longer to create and uses more storage."
                }
                isSelected={selected === "snapshot"}
                onClick={() => setSelected("snapshot")}
                isDisabled={isDisabled}
                badge={hasBadge && <LicenseRestrictedBadge licenseRequired="Enterprise" className="ms-2" />}
                popoverMessage={hasPopover ? "Good when a fast restore matters more than storage space." : undefined}
            />
        </div>
    );
}

export default {
    title: "Bits/SelectableCard",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        selectedCard: { control: "radio", options: ["none", "backup", "snapshot"] },
    },
    args: {
        selectedCard: "none",
        isDisabled: false,
        hasBadge: false,
        hasPopover: true,
        hasLongValues: false,
        containerWidth: 926,
    },
    render: (args) => <SelectableCardStory {...args} />,
} satisfies Meta<SelectableCardStoryArgs>;

type Story = StoryObj<SelectableCardStoryArgs>;

export const Default: Story = {};

export const Selected: Story = {
    args: { selectedCard: "snapshot" },
};

export const Disabled: Story = {
    args: { isDisabled: true, hasBadge: true },
};

export const LongValues: Story = {
    args: { hasLongValues: true, hasBadge: true },
};

export const Narrow: Story = {
    args: { hasLongValues: true, containerWidth: 360 },
};
