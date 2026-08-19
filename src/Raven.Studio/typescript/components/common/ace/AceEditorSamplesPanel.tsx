import React from "react";
import Button from "react-bootstrap/Button";
import { AnimatePresence, motion } from "motion/react";
import { Icon } from "components/common/Icon";
import SamplesTabs from "components/common/sampleQueries/SamplesTabs";
import { SamplesTab } from "components/common/sampleQueries/partials/sampleQueriesTypes";

export interface AceEditorSamplesPanelConfig {
    tabs: SamplesTab[];
    tooltipTitle?: string;
}

interface AceEditorSamplesToggleActionProps {
    tooltipTitle?: string;
    onClick: () => void;
}

export function AceEditorSamplesToggleAction({
    tooltipTitle = "Browse samples",
    onClick,
}: AceEditorSamplesToggleActionProps) {
    return (
        <Button size="sm" title={tooltipTitle} onClick={onClick} className="p-0 text-reset" variant="link">
            <Icon icon="help" margin="m-0" />
        </Button>
    );
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
