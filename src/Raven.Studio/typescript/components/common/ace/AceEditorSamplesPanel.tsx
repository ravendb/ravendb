import React from "react";
import Button from "react-bootstrap/Button";
import { AnimatePresence, motion } from "motion/react";
import { Icon } from "components/common/Icon";
import SamplesTabs from "components/common/samples/SamplesTabs";
import { SamplesTab } from "components/common/samples/partials/samplesTypes";
import { ConfirmOptions } from "components/common/ConfirmDialog";

export interface AceEditorSamplesPanelConfig {
    tabs: SamplesTab[];
    isOpen?: boolean;
    onToggle?: () => void;
}

interface AceEditorSamplesToggleActionProps {
    onClick: () => void;
}

export function AceEditorSamplesToggleAction({ onClick }: AceEditorSamplesToggleActionProps) {
    return (
        <Button size="sm" title="Browse samples" onClick={onClick} className="p-0 text-reset" variant="link">
            <Icon icon="help" margin="m-0" />
        </Button>
    );
}

export async function confirmSampleLoad(
    confirm: (options: ConfirmOptions) => Promise<boolean>,
    currentEditorValue: string
): Promise<boolean> {
    if (!currentEditorValue?.trim() || !confirm) {
        return true;
    }

    return confirm({
        title: "Load sample into the editor?",
        message: "The current editor content will be replaced.",
        actionColor: "warning",
        confirmText: "Load",
    });
}

interface AceEditorSamplesPanelProps {
    isOpen: boolean;
    tabs: SamplesTab[];
    onSelect: (script: string) => void;
    onClose: () => void;
}

export default function AceEditorSamplesPanel({ isOpen, tabs, onSelect, onClose }: AceEditorSamplesPanelProps) {
    return (
        <AnimatePresence>
            {isOpen && (
                <motion.div
                    className="ace-samples-panel bs5"
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: "auto" }}
                    exit={{ opacity: 0, height: 0 }}
                    transition={{ duration: 0.2 }}
                    style={{ overflow: "hidden" }}
                >
                    <SamplesTabs tabs={tabs} onSelect={onSelect} onClose={onClose} />
                </motion.div>
            )}
        </AnimatePresence>
    );
}
